package Method;

import ArgsConfig.JsonMap;
import Common.Converter;
import Common.EventPattern;
import Common.EventSchema;
import Common.MatchStrategy;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * We found that reading records from the store is faster than reading data from strings file.
 * In order to make the comparison more fair,
 * we used this method in our experiment
 *
 * Step 1: read file from string file
 * Step 2: store record binary file
 */
public class FullScanFromStore {
    public static boolean debug = true;
    private int recordIndices;
    private final EventSchema schema;

    public FullScanFromStore(EventSchema schema) {
        recordIndices = 0;
        this.schema = schema;
    }

    public boolean insertRecord(String record){
        String[] splits = record.split(",");

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        store.insertByteRecord(bytesRecord);
        recordIndices++;
        return true;
    }

    public int countQuery(EventPattern pattern, AbstractJoinStrategy join){
        MatchStrategy strategy = pattern.getStrategy();
        long startTime1 = System.nanoTime();
        List<List<byte[]>> buckets = getRecordUsingIC(pattern);
        long endTime1 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0) / 1_000_000);
            System.out.println("scan cost: " + output + "ms");
        }

        int ans;
        long startTime2 = System.nanoTime();
        switch (strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.countUsingS2WithBytes(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.countUsingS3WithBytes(pattern, buckets);
            default -> {
                System.out.println("do not support this strategy, default is SKIP_TILL_ANY_MATCH");
                ans = join.countUsingS2WithBytes(pattern, buckets);
            }
        }
        long endTime2 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime2 - startTime2 + 0.0) / 1_000_000);
            System.out.println("join cost: " + output + "ms");
        }
        return ans;
    }

    public List<Tuple> tupleQuery(EventPattern pattern, AbstractJoinStrategy join){
        MatchStrategy strategy = pattern.getStrategy();
        List<List<byte[]>> buckets = getRecordUsingIC(pattern);
        List<Tuple> ans;
        switch (strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.getTupleUsingS3WithBytes(pattern, buckets);
            default -> {
                System.out.println("do not support this strategy, default is SKIP_TILL_ANY_MATCH");
                ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            }
        }
        return ans;
    }

    public List<List<byte[]>> getRecordUsingIC(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqVarNames.length;

        List<List<byte[]>> buckets = new ArrayList<>();
        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        int recordSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();
        int typeIdx = schema.getTypeIdx();
        int curPage = 0;
        int curOffset = 0;
        int pageSize = store.getPageSize();

        // store event arrivals
        HashMap<String, Integer> map = new HashMap<>();
        byte[] firstRecord = null;
        byte[] lastRecord = null;
        // access all records
        for(int i = 0; i < recordIndices; ++i){
            if(curOffset + recordSize > pageSize){
                curPage++;
                curOffset = 0;
            }
            RID rid = new RID(curPage, curOffset);
            curOffset += recordSize;
            // read record from store
            byte[] record = store.readByteRecord(rid);

            if(firstRecord == null){
                firstRecord = record;
            }
            lastRecord = record;

            String curType = schema.getTypeFromBytesRecord(record, typeIdx);
            map.put(curType, map.getOrDefault(curType,0) + 1);

            for(int j = 0; j < patternLen; ++j) {
                // Firstly, the event types must be equal.
                // Once equal, check if the predicate satisfies the constraint conditions.
                // If so, place it in the i-th bucket
                if (curType.equals(seqEventTypes[j])) {
                    String varName = seqVarNames[j];
                    List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                    boolean satisfy = true;
                    // 检查是否满足独立谓词约束
                    if(icList != null){
                        for (IndependentConstraint ic : icList) {
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // obtain the corresponding column for storage based on the attribute name
                            int col = schema.getAttrNameIdx(name);
                            long value = schema.getValueFromBytesRecord(record, col);
                            if (value < min || value > max) {
                                satisfy = false;
                                break;
                            }
                        }
                    }
                    if (satisfy) {
                        buckets.get(j).add(record);
                    }
                }
            }
        }

        // store the arrivals to json file
        int timeIdx = schema.getTimestampIdx();
        HashMap<String, Double> arrivals = new HashMap<>(map.size());
        long firstTimestamp = Converter.bytesToLong(schema.getIthAttrBytes(firstRecord, timeIdx));
        long lastTimestamp = Converter.bytesToLong(schema.getIthAttrBytes(lastRecord, timeIdx));
        long span = lastTimestamp - firstTimestamp;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String key = entry.getKey();
            double value = ((double) entry.getValue()) / span;
            arrivals.put(key, value);
        }
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        JsonMap.arrivalMapToJson(arrivals, jsonFilePath);

        return buckets;
    }
}
