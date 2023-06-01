package Method;

import Common.IndexValuePair;
import Common.EventPattern;
import Common.MatchStrategy;
import Common.ReplayIntervals;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import RTree.MemoryRTree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NaiveRTreePlus extends Index {
    public static boolean debug = true;                 // print
    private MemoryRTree rTree;

    @Override
    public void initial(){
        // Initialize R-tree
        rTree = new MemoryRTree(indexAttrNum + 1);
        // Currently, there is no analysis of its cost function
        // it is not necessary to use a reservoir
    }
    public NaiveRTreePlus(String indexName){
        super(indexName);
    }

    @Override
    public boolean insertRecord(String record) {
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();

        String[] splits = record.split(",");
        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum + 1];

        // Only indexed attributes will be placed in attrValArray
        for (int i = 0; i < splits.length; ++i) {
            if (attrTypes[i].equals("TYPE")) {
                eventType = splits[i];
            } else if (attrTypes[i].equals("TIMESTAMP")) {
                timestamp = Long.parseLong(splits[i]);
            } else if (attrTypes[i].equals("INT")) {
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    attrValArray[idx] = Long.parseLong(splits[i]);
                }
            } else if (attrTypes[i].contains("FLOAT")) {
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Float.parseFloat(splits[i]) * magnification);
                }
            } else if (attrTypes[i].contains("DOUBLE")) {
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Double.parseDouble(splits[i]) * magnification);
                }
            } else {
                System.out.println("Don't support this data type.");
            }
        }

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        IndexValuePair value = new IndexValuePair(timestamp, rid);
        int typeId = schema.getTypeId(eventType);
        attrValArray[indexAttrNum] = typeId;

        // insert index attribute to r tree
        rTree.insert(attrValArray, value);

        autoIndices++;
        return true;
    }
    @Override
    public boolean insertBatchRecord(String[] records){
        return false;
    }

    @Override
    public int countQuery(EventPattern pattern, AbstractJoinStrategy join){
        String[] seqVarNames = pattern.getSeqVarNames();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        int patternLen = seqVarNames.length;
        long startTime0 = System.nanoTime();

        record RecordNum(int recordNum, int varId){}
        List<RecordNum> numbers = new ArrayList<>(patternLen);
        // step1: find all events
        List<List<IndexValuePair>> pairsList = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            String eventType = seqEventTypes[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<IndexValuePair> curPairs = getAllPairs(eventType, icList);
            pairsList.add(curPairs);
            numbers.add(new RecordNum(curPairs.size(), i));
        }

        // step2: generate replay intervals
        numbers.sort(Comparator.comparingInt(RecordNum::recordNum));
        int minNumPos = numbers.get(0).varId();
        ReplayIntervals intervals = new ReplayIntervals();
        long startOffset, endOffset;
        long tau = pattern.getTau();
        List<IndexValuePair> minPairs = pairsList.get(minNumPos);
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
        for(IndexValuePair pair : minPairs){
            long curTime = pair.timestamp();
            intervals.insert(curTime + startOffset, curTime + endOffset);
        }

        // step 3: using replay interval filtering
        List<IthRID> queryRIDList = getRIDList(pairsList.get(minNumPos), minNumPos);

        for(int i = 1; i < patternLen; ++i){
            int curVarId = numbers.get(i).varId();
            List<IndexValuePair> curPairs = pairsList.get(curVarId);
            List<IthRID> curRIDList = new ArrayList<>();
            // pointer position rewind to 0
            intervals.rewind();
            for(IndexValuePair pair : curPairs){
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

        // get the buckets
        long startTime1 = System.nanoTime();
        List<List<byte[]>> buckets = getRecordBuckets(queryRIDList, patternLen);
        long endTime1 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0) / 1_000_000);
            System.out.println("scan cost: " + output + "ms");
        }

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
    public List<Tuple> tupleQuery(EventPattern pattern, AbstractJoinStrategy join){
        String[] seqVarNames = pattern.getSeqVarNames();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        int patternLen = seqVarNames.length;
        // 这个桶是用做Join的

        record RecordNum(int recordNum, int varId){}
        List<RecordNum> numbers = new ArrayList<>(patternLen);
        // step1: find all events
        List<List<IndexValuePair>> pairsList = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            String eventType = seqEventTypes[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<IndexValuePair> curPairs = getAllPairs(eventType, icList);
            pairsList.add(curPairs);
            numbers.add(new RecordNum(curPairs.size(), i));
        }

        // step2: generate replay intervals
        numbers.sort(Comparator.comparingInt(RecordNum::recordNum));
        int minNumPos = numbers.get(0).varId();
        ReplayIntervals intervals = new ReplayIntervals();
        long startOffset, endOffset;
        long tau = pattern.getTau();
        List<IndexValuePair> minPairs = pairsList.get(minNumPos);
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
        for(IndexValuePair pair : minPairs){
            long curTime = pair.timestamp();
            intervals.insert(curTime + startOffset, curTime + endOffset);
        }

        // step 3: using replay interval filtering
        List<IthRID> queryRIDList = getRIDList(pairsList.get(minNumPos), minNumPos);

        for(int i = 1; i < patternLen; ++i){
            int curVarId = numbers.get(i).varId();
            List<IndexValuePair> curPairs = pairsList.get(curVarId);
            List<IthRID> curRIDList = new ArrayList<>();
            // pointer position rewind to 0
            intervals.rewind();
            for(IndexValuePair pair : curPairs){
                long t = pair.timestamp();
                if(intervals.include(t)){
                    curRIDList.add(new IthRID(pair.rid(), curVarId));
                }
            }
            queryRIDList = mergeRID(queryRIDList, curRIDList);
        }
        List<List<byte[]>> buckets = getRecordBuckets(queryRIDList, patternLen);

        List<Tuple> ans;
        MatchStrategy strategy = pattern.getStrategy();
        switch(strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.getTupleUsingS3WithBytes(pattern, buckets);
            default -> {
                System.out.println("this strategy do not support, default choose S2");
                ans = join.getTupleUsingS2WithBytes(pattern, buckets);
            }
        }
        return ans;
    }

    @Override
    public void print(){
        System.out.println("----------------Index Information----------------");
        System.out.println("Index name: '" + getIndexName() + "'" + " schema name: '" + schema.getSchemaName() + "'");
        System.out.print("Index attribute name:" );
        for(String indexAttrName : getIndexAttrNames()){
            System.out.print(" '" + indexAttrName + "'");
        }
        System.out.println("\nrecordNum: " + autoIndices);
    }

    /**
     * get variable index value list
     * @param eventType event type
     * @param icList    The independent constraint corresponding to this event
     * @return          index value pair that hold the constraints
     */
    public final List<IndexValuePair> getAllPairs(String eventType, List<IndependentConstraint> icList){
        int typeId = schema.getTypeId(eventType);
        List<IndexValuePair> ans;

        long[] min = new long[indexAttrNum + 1];
        long[] max = new long[indexAttrNum + 1];

        for(int i = 0; i < indexAttrNum + 1; ++i){
            min[i] = Long.MIN_VALUE;
            max[i] = Long.MAX_VALUE;
        }

        min[indexAttrNum] = typeId;
        max[indexAttrNum] = typeId;

        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            if (indexAttrNameMap.containsKey(attrName)) {
                // idx represents the storage position corresponding to the index attribute name
                int idx = indexAttrNameMap.get(attrName);
                min[idx] = ic.getMinValue();
                max[idx] = ic.getMaxValue();
            } else {
                System.out.println("This attribute do not have an index.");
            }
        }

        ans = rTree.rangeQuery(min, max);

        if(ans != null){
            ans.sort((o1, o2) -> {
                RID rid1 = o1.rid();
                RID rid2 = o2.rid();
                if(rid1.page() == rid2.page()){
                    return rid1.offset() - rid2.offset();
                }else{
                    return rid1.page() - rid2.page();
                }
            });
        }

        return ans;
    }

    public final List<IthRID> getRIDList(List<IndexValuePair> list, int ith){
        if(list == null){
            return null;
        }
        List<IthRID> ans = new ArrayList<>();
        for(IndexValuePair triple : list){
            IthRID ithTID = new IthRID(triple.rid(), ith);
            ans.add(ithTID);
        }
        return ans;
    }

    public final List<IthRID> mergeRID(List<IthRID> a, List<IthRID> b){
        // a and b is sorted by timestamp
        if(a == null){
            return b;
        }
        if(b == null){
            return a;
        }

        int size1 = a.size();
        int size2 = b.size();

        List<IthRID> ans = new ArrayList<>(size1 + size2);
        int i=0;
        int j = 0;

        while(i < size1 && j < size2)
        {
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
    public final List<List<byte[]>> getRecordBuckets(List<IthRID> ithRIDList, int patternLen){
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

        return buckets;
    }
}
