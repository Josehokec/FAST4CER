package Method;

import ArgsConfig.JsonMap;
import Common.*;
import Condition.IndependentConstraint;
import Condition.IndependentConstraintQuad;
import FastModule.EventBuffers;
import FastModule.FastTemporaryTriple;
import FastModule.IndexPartition;
import FastModule.RidTimePair;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;

import java.io.File;
import java.util.*;

/**
 * fast index = buffer + data partition
 * Note that bitmaps can only store non-negative integers. <br>
 * If the index value is a floating point number, <br>
 * it needs to be converted to an integer for storage. <br>
 * If the index value is less than 0, it needs to convert the value.<br>
 */
public class FASTIndex extends Index {
    public static boolean debug = true;
    private List<Long> attrMinValues;                       // minimum values for indexed attribute
    private List<Long> attrMaxRange;                        // maximum values for indexed attribute
    private HashMap<String, List<Integer>> partitionTable;  //
    private List<IndexPartition> indexPartitions;           // all data partition
    private EventBuffers buffers;                           // buffers

    public FASTIndex(String schemaName){
        super(schemaName);
    }

    @Override
    public void initial() {
        setMinMaxArray();
        partitionTable = new HashMap<>();
        indexPartitions = new ArrayList<>(32);
        buffers = new EventBuffers(indexAttrNum, attrMinValues, attrMaxRange);
        // Set the arrival rate for each event, which is later used to calculate the selection rate
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);
        // sampling
        reservoir = new ReservoirSampling(indexAttrNum);
    }

    /**
     * fast index need to
     * set the min value array and max range array
     */
    public final void setMinMaxArray(){
        attrMinValues = new ArrayList<>(indexAttrNum);
        attrMaxRange = new ArrayList<>(indexAttrNum);

        String[] indexAttrNames = getIndexAttrNames();
        for(int i = 0; i < indexAttrNum; ++i){
            String name = indexAttrNames[i];
            int idx = schema.getAttrNameIdx(name);
            attrMinValues.add(schema.getIthAttrMinValue(idx));
            attrMaxRange.add(schema.getIthAttrMaxValue(idx) - attrMinValues.get(i));
        }
    }

    @Override
    public boolean insertRecord(String record) {
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();

        String[] splits = record.split(",");

        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum];

        // 只有是索引的属性才会被放入到attrValArray中
        for(int i = 0; i < splits.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                eventType = splits[i];
            }else if(attrTypes[i].equals("TIMESTAMP")){
                timestamp = Long.parseLong(splits[i]);
            }else if(attrTypes[i].equals("INT") ){
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    attrValArray[idx] = Long.parseLong(splits[i]);
                }
            }else if(attrTypes[i].contains("FLOAT")) {
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Float.parseFloat(splits[i]) * magnification);
                }
            } else if(attrTypes[i].contains("DOUBLE")){
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Double.parseDouble(splits[i]) * magnification);
                }
            }else{
                throw new RuntimeException("Don't support this data type: " + attrTypes[i]);
            }
        }

        // The reservoir stores unchanged values
        reservoir.sampling(attrValArray, autoIndices);

        // 由于RangeBitmap只能存储非负数整数，因此需要变换变换后的值是: y = x - min
        for(int i = 0; i < indexAttrNum; ++i){
            attrValArray[i] -= attrMinValues.get(i);
        }

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        FastTemporaryTriple triple = new FastTemporaryTriple(timestamp, rid, attrValArray);

        // insert triple to buffer
        IndexPartition indexPartition = buffers.insertRecord(eventType, triple);
        if(indexPartition != null){
            // has construct a data partition
            int partitionId = indexPartitions.size();
            Set<String> keys = indexPartition.getInfoMapKeySet();
            // key is event type
            for(String key : keys){
                if(partitionTable.containsKey(key)){
                    partitionTable.get(key).add(partitionId);
                }else{
                    List<Integer> list = new ArrayList<>(16);
                    list.add(partitionId);
                    partitionTable.put(key, list);
                }
            }
            indexPartitions.add(indexPartition);
        }

        // update indices
        autoIndices++;
        return true;
    }

    @Override
    public boolean insertBatchRecord(String[] record) {
        return false;
    }

    @Override
    public int countQuery(EventPattern pattern, AbstractJoinStrategy join) {
        //System.out.println("indexPartitions numbers: " + indexPartitions.size());
        //System.out.println("buffers size: " + buffers.getRecordNum());
        List<List<byte[]>> buckets = greedyFindRecordUsingSel(pattern);
        int ans;
        long startTime2 = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        switch(strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.countUsingS2WithBytes(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.countUsingS3WithBytes(pattern, buckets);
            default -> {
                System.out.println("this strategy do not support, default choose S2");
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

    @Override
    public List<Tuple> tupleQuery(EventPattern pattern, AbstractJoinStrategy join) {
        List<List<byte[]>> buckets = greedyFindRecordUsingSel(pattern);
        List<Tuple> ans;
        long startTime2 = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        switch(strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.getTupleUsingS3WithBytes(pattern, buckets);
            default -> {
                System.out.println("this strategy do not support, default choose S2");
                ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            }
        }
        long endTime2 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime2 - startTime2 + 0.0) / 1_000_000);
            System.out.println("join cost: " + output + "ms");
        }
        return ans;
    }

    @Override
    public void print() {

    }

    /**
     * Greedily selecting the variable with the lowest selection rate for querying
     * @param pattern       query pattern
     * @return              buckets
     */
    public final List<List<byte[]>> greedyFindRecordUsingSel(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        final int patternLen = seqEventTypes.length;
        //record SelectivityIndexPair(double sel, int varId){}

        // variable v_i overall selectivity
        List<Double> varSelList = new ArrayList<>(patternLen);
        double sumArrival = 0;
        for(double a : arrivals.values()){
            sumArrival += a;
        }
        // calculate the overall selection rate for each variable
        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            double sel = arrivals.get(seqEventTypes[i]) / sumArrival;
            if(icList != null){
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
                }
            }
            varSelList.add(sel);
        }

        // ----------------------------------------------------------
        long startTime0 = System.nanoTime();
        record RecordNum(int recordNum, int varId){}
        List<RecordNum> numbers = new ArrayList<>(patternLen);
        List<List<RidTimePair>> pairsList = new ArrayList<>(patternLen);
        for(int i = 0; i < patternLen; ++i){
            String type = seqEventTypes[i];
            String varName = seqVarNames[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<RidTimePair> curPairs = getRidTimePairs(type, icList);
            pairsList.add(curPairs);
            numbers.add(new RecordNum(curPairs.size(), i));
        }

        // step2: generate replay intervals
        numbers.sort(Comparator.comparingInt(RecordNum::recordNum));
        int minNumPos = numbers.get(0).varId();
        ReplayIntervals intervals = new ReplayIntervals();
        long startOffset, endOffset;
        long tau = pattern.getTau();
        List<RidTimePair> minPairs = pairsList.get(minNumPos);
        if(minNumPos == 0){
            startOffset = 0;
            endOffset = tau;
        }else if(minNumPos == patternLen - 1){
            startOffset = -tau;
            endOffset = 0;
        }else{
            startOffset = -tau;
            endOffset = tau;
        }

        for(RidTimePair pair : minPairs){
            long curTime = pair.timestamp();
            intervals.insert(curTime + startOffset, curTime + endOffset);
        }

        // step 3: using replay interval filtering
        List<IthRID> queryRIDList = getRIDList(pairsList.get(minNumPos), minNumPos);

        for(int i = 1; i < patternLen; ++i){
            int curVarId = numbers.get(i).varId();
            List<RidTimePair> curPairs = pairsList.get(curVarId);
            List<IthRID> curRIDList = new ArrayList<>();
            // pointer position rewind to 0
            intervals.rewind();
            for(RidTimePair pair : curPairs){
                long t = pair.timestamp();
                if(intervals.include(t)){
                    curRIDList.add(new IthRID(pair.rid(), curVarId));
                }
            }
            queryRIDList = mergeRID(queryRIDList, curRIDList);
        }

        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0) / 1_000_000);
            System.out.println("filter cost: " + output + "ms");
        }

        return getRecordBuckets(queryRIDList, patternLen);
    }


    /**
     * get <rid, timestamp> pair
     * @param eventType     event type
     * @param icList        all independent constraints
     * @return              RidTimePairs
     */
    public final List<RidTimePair> getRidTimePairs(String eventType, List<IndependentConstraint> icList){
        List<RidTimePair> pairs = new ArrayList<>();
        final int icNum = icList.size();
        List<IndependentConstraintQuad> icQuads = new ArrayList<>(icNum);
        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();

            IndependentConstraintQuad quad;
            switch (mark) {
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                case 1 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue());
                case 2 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx));
                case 3 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx));
                default -> {
                    System.out.println("mark value: " + mark + " , it is anomaly");
                    quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                            ic.getMaxValue() - attrMinValues.get(idx));
                }
            }
            icQuads.add(quad);
        }

        // getRidTimePairs from data partition
        long startTime0 = System.nanoTime();
        if(partitionTable.containsKey(eventType)) {
            List<Integer> partitionIds = partitionTable.get(eventType);
            for(int id : partitionIds) {
                IndexPartition indexPartition = indexPartitions.get(id);
                List<RidTimePair> curPairs = indexPartition.query(eventType, icQuads);
                pairs.addAll(curPairs);
            }
        }
        long endTime0 = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0) / 1_000_000);
            System.out.println("partition query cost: " + output + "ms");
        }

        // getRidTimePairs from buffer
        long startTime1 = System.nanoTime();
        List<RidTimePair> pairFromBuffer = buffers.query(eventType, icQuads);
        pairs.addAll(pairFromBuffer);
        long endTime1 = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0)/1_000_000);
            System.out.println("buffer query cost: " + output + "ms");
        }

        return pairs;
    }

    /**
     * Obtain all events that meet the independent predicate constraint of the i-th variable <br>
     * and then filter using replay intervals
     * @param eventType         event type of i-th variable in query pattern
     * @param icList            independent constraint list
     * @param ith               variable position
     * @param replayIntervals   replay intervals
     * @return                  ith rid list
     */
    public final List<IthRID> getRIDList(String eventType, List<IndependentConstraint> icList, int ith, ReplayIntervals replayIntervals){
        List<RidTimePair> pairs = getRidTimePairs(eventType, icList);
        return usingReplayIntervalsFilter(pairs, ith, replayIntervals);
    }

    public final List<IthRID> getRIDList(List<RidTimePair> list, int ith){
        if(list == null){
            return null;
        }
        List<IthRID> ans = new ArrayList<>();
        for(RidTimePair pair : list){
            IthRID ithTID = new IthRID(pair.rid(), ith);
            ans.add(ithTID);
        }
        return ans;
    }

    /**
     * use replay intervals to filter records that are not within the timestamp range
     * @param pairs             rid and timestamp pair
     * @param ith               var position
     * @param replayIntervals   replay intervals
     * @return                  ithRIDList
     */
    public final List<IthRID> usingReplayIntervalsFilter(List<RidTimePair> pairs, int ith, ReplayIntervals replayIntervals){
        long startTime = System.nanoTime();
        List<IthRID> ans = new ArrayList<>();
        // pointer position rewind to 0
        replayIntervals.rewind();
        for(RidTimePair pair : pairs){
            long t = pair.timestamp();
            if(replayIntervals.include(t)){
                ans.add(new IthRID(pair.rid(), ith));
            }
        }

        long endTime = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime - startTime + 0.0) / 1_000_000);
            System.out.println("replay filter cost: " + output + "ms");
        }

        return ans;
    }

    /**
     * merge two sorted IthRID
     * @param a     list1
     * @param b     list2
     * @return      mergeList
     */
    public final List<IthRID> mergeRID(List<IthRID> a, List<IthRID> b){
        if(a == null || b == null){
            return a == null ? b : a;
        }

        final int size1 = a.size();
        final int size2 = b.size();

        List<IthRID> ans = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while(i < size1 && j < size2) {
            RID rid1 = a.get(i).rid();
            RID rid2 = b.get(j).rid();
            if(rid1.compareTo(rid2) <= 0){
                ans.add(a.get(i++));
            }else{
                ans.add(b.get(j++));
            }
        }

        while(i < size1){
            ans.add(a.get(i++));
        }

        while(j < size2){
            ans.add(b.get(j++));
        }

        return ans;
    }

    /**
     * step 1: according rid to get byte record<br>
     * step 2: filter buffer data
     * @param ithRIDList        sumRIDList
     * @param patternLen        length of pattern
     * @return                  buckets
     */
    public final List<List<byte[]>> getRecordBuckets(List<IthRID> ithRIDList, int patternLen){
        // debug
        long startTime = System.nanoTime();
        EventStore store = schema.getStore();
        List<List<byte[]>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        for(IthRID ithRID : ithRIDList){
            int ith = ithRID.ith();
            RID rid = ithRID.rid();
            byte[] bytesRecord = store.readByteRecord(rid);
            buckets.get(ith).add(bytesRecord);
        }
        long endTime = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime - startTime + 0.0)/1_000_000);
            System.out.println("scan cost: " + output + "ms");
        }
        return buckets;
    }
}
