package Method;

import ArgsConfig.JsonMap;
import Common.*;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import SkipList.SkipList;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NaiveSkipListPlus extends Index {
    public static boolean debug = true;                 // print
    public static boolean testCost = false;             // whether we need to test cost const value
    private List<SkipList<IndexValuePair>> skipLists;
    private double sumArrival;

    @Override
    public void initial(){
        // index attribute 1, ..., index attribute d, type index
        skipLists = new ArrayList<>(indexAttrNum + 1);
        for(int i = 0; i < indexAttrNum + 1; ++i){
            skipLists.add(new SkipList<IndexValuePair>());
        }

        // Initialize Reservoir
        reservoir = new ReservoirSampling(indexAttrNum);

        // Read the arrival rate of each event
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);

        sumArrival = 0;
        for(double arrival : arrivals.values()){
            sumArrival += arrival;
        }

        // Initialization of cost
        if(testCost){
            List<Double> costs1 = GetCostConst.getSchemaTreeCosts(schema);
            ConstCosts.setIntervalScanCosts(costs1);
        }else{
            ConstCosts.setConstCosts();
        }
    }
    public NaiveSkipListPlus(String indexName){
        super(indexName);
    }

    @Override
    public boolean insertRecord(String record) {
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();

        String[] splits = record.split(",");
        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum];

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

        // sampling
        reservoir.sampling(attrValArray, autoIndices);

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);

        IndexValuePair value = new IndexValuePair(timestamp, rid);
        int typeId = schema.getTypeId(eventType);
        skipLists.get(indexAttrNum).insert(typeId, value);

        // key is attribute value，value is pair (timestamp, rid)
        for(int i = 0; i < indexAttrNum; ++i){
            skipLists.get(i).insert(attrValArray[i], value);
        }

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

        List<List<SelectivityPair>> alphas = new ArrayList<>(patternLen);
        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            String eventType = seqEventTypes[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            // evaluate selectivity
            List<SelectivityPair> selPairs = new ArrayList<>();
            double alpha0 = arrivals.get(eventType) / sumArrival;
            selPairs.add(new SelectivityPair(-1, alpha0));

            if(icList != null){
                for(int j = 0; j < icList.size(); ++j){
                    IndependentConstraint ic = icList.get(j);
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    long min = ic.getMinValue();
                    long max = ic.getMaxValue();
                    double sel = reservoir.selectivity(idx, min, max);
                    selPairs.add(new SelectivityPair(j, sel));
                }
            }
            alphas.add(selPairs);
        }

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

        if(!debug){
            double sumCost = evaluateCost(alphas);
            String output = String.format("%.3f", sumCost / 1_000_000);
            System.out.println("predicated cost: " + output + "ms");
        }
        // get the buckets
        List<List<byte[]>> buckets = getRecordBuckets(queryRIDList, patternLen);

        long startTime2 = System.nanoTime();
        int ans;
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

        // 这个桶是用做Join的
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

    public final double evaluateCost(List<List<SelectivityPair>> alphas){
        double ans = 0;
        for (List<SelectivityPair> selPairs : alphas) {
            int needMerge = selPairs.size() == 1 ? 1 : 0;
            double CSI = 0;
            double sumSel = 1;
            for (SelectivityPair selPair : selPairs) {
                double curSel = selPair.selectivity();
                sumSel *= curSel;
                double nIJ = autoIndices * curSel;
                double curIndexCost = nIJ * (ConstCosts.C_LIST + ConstCosts.C_SORT * Math.log(nIJ) / Math.log(2)
                        + needMerge * ConstCosts.C_MERGE);
                CSI += curIndexCost;
            }

            double nIPage = schema.getPageStoreRecordNum() * sumSel;
            double nPage;
            if (nIPage < 1) {
                nPage = autoIndices * sumSel;
            } else {
                nPage = (autoIndices + 0.0) / schema.getPageStoreRecordNum();
            }
            double CAD = nPage * ConstCosts.C_PAGE;
            ans += (CSI + CAD);
        }

        return ans;
    }

    /**
     * get variable index value list
     * @param eventType event type
     * @param icList    The independent constraint corresponding to this event
     * @return          index value pair that hold the constraints
     */
    public final List<IndexValuePair> getAllPairs(String eventType, List<IndependentConstraint> icList){
        // step 1: using event type to filter
        int typeId = schema.getTypeId(eventType);
        List<IndexValuePair> ans = skipLists.get(indexAttrNum).rangeQuery(typeId, typeId);
        ans.sort((o1, o2) -> {
            RID rid1 = o1.rid();
            RID rid2 = o2.rid();
            if(rid1.page() == rid2.page()){
                return rid1.offset() - rid2.offset();
            }else{
                return rid1.page() - rid2.page();
            }
        });

        if(icList != null){
            // step 2: using independent constraints to filter
            for (IndependentConstraint ic : icList) {
                String attrName = ic.getAttrName();
                // idx represents the storage position corresponding to the index attribute name
                int idx = -1;
                if (indexAttrNameMap.containsKey(attrName)) {
                    idx = indexAttrNameMap.get(attrName);
                } else {
                    System.out.println("This attribute do not have an index.");
                }

                int mark = ic.hasMinMaxValue();
                if (idx == -1 || mark == 0) {
                    System.out.print("Class FastIndex - this attribute does not have index.");
                }else{
                    List<IndexValuePair> temp = skipLists.get(idx).rangeQuery(ic.getMinValue(), ic.getMaxValue());
                    //first sort then combine
                    temp.sort((o1, o2) -> {
                        RID rid1 = o1.rid();
                        RID rid2 = o2.rid();
                        if(rid1.page() == rid2.page()){
                            return rid1.offset() - rid2.offset();
                        }else{
                            return rid1.page() - rid2.page();
                        }
                    });

                    ans = intersect(ans, temp);
                }
            }
        }

        return ans;
    }

    /**
     * Intersection of two lists
     * @param list1 1-th list
     * @param list2 2-th list
     * @return Intersection of two lists
     */
    public final List<IndexValuePair> intersect(List<IndexValuePair> list1, List<IndexValuePair> list2){
        if(list1 == null || list2 == null){
            return null;
        }else{
            List<IndexValuePair> ans = new ArrayList<>();
            int idx1 = 0;
            int idx2 = 0;
            while(idx1 < list1.size() && idx2 < list2.size()){
                RID rid1 = list1.get(idx1).rid();
                RID rid2 = list2.get(idx2).rid();
                if(rid1.page() == rid2.page() && rid1.offset() == rid2.offset()){
                    ans.add(list1.get(idx1));
                    idx1++;
                    idx2++;
                }else if(rid1.compareTo(rid2) < 0){
                    idx1++;
                }else{
                    idx2++;
                }
            }
            return ans;
        }
    }

    /**
     * @param list          index value list
     * @return              byte record list
     */
    public final List<byte[]> getRecordList(List<IndexValuePair> list){
        if(list == null){
            return null;
        }
        EventStore store = schema.getStore();
        List<byte[]> ans = new ArrayList<>();
        for(IndexValuePair triple : list){
            RID rid = triple.rid();
            byte[] bytesRecord = store.readByteRecord(rid);
            ans.add(bytesRecord);
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
        // a和b是有序的
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
            String output = String.format("%.3f", (endTime - startTime + 0.0) / 1_000_000);
            System.out.println("scan cost: " + output + "ms");
        }
        return buckets;
    }
}
