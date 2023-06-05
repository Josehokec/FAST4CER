package Method;

import ArgsConfig.JsonMap;
import BPlusTree.MemoryBPlusTree;
import Common.*;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;
import SkipList.SkipList;
import Store.EventStore;
import Store.RID;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * State-of-the-art algorithm:
 * 1. Index accelerated pattern matching in event stores [SIGMOD'21]            <br>
 * 2. High-Performance Row Pattern Recognition Using Joins [VLDB'23]            <br>
 * The original version of the algorithm mainly dealt with S1 strategy,         <br>
 * which using B+trees as the primary index and LSM-Tree as the secondary index <br>
 * Please note that we conducted the index experiment in memory                 <br>
 * The performance of the b+tree in memory is very low, so we made changes:     <br>
 * Primary index uses skip list, secondary indexes use B plus tree              <br>
 * Index contains：indexAttrNum recordIndices schema arrivals reservoir indexAttrNameMap
 */
public class IntervalScan extends Index {
    public static boolean debug = true;                 // print
    public static boolean testCost = false;             // whether we need to test cost const value
    private double sumArrival;                          // arrival rate of all events
    private SkipList<RID> primaryIndex;                 // the key of the primary index is the timestamp, and the value is the RID
    private List<MemoryBPlusTree> secondaryIndexes;     // the key of the secondary index is the timestamp
    public IntervalScan(String indexName) {
        super(indexName);
    }

    /**
     * construct secondary indexes (event type attribute are also included)
     * construct primary index
     * reservoir Sampling
     * calculate the total arrival rate
     * initialize various expenses
     */
    @Override
    public void initial() {
        // Initializing the secondary index assumes that there are d attributes in the index,
        // and a total of d+1 indices need to be constructed because the attribute type needs to be constructed
        secondaryIndexes = new ArrayList<>(indexAttrNum + 1);
        for (int i = 0; i < indexAttrNum + 1; ++i) {
            secondaryIndexes.add(new MemoryBPlusTree());
        }
        // Initialize primary index
        primaryIndex = new SkipList<RID>();
        // Initialize Reservoir
        reservoir = new ReservoirSampling(indexAttrNum);
        // 读取每个事件到达率
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

        // Reservoir sampling
        reservoir.sampling(attrValArray, autoIndices);

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);

        // Insert event types into the secondary index
        IndexValuePair value = new IndexValuePair(timestamp, rid);
        long typeId = schema.getTypeId(eventType);
        secondaryIndexes.get(indexAttrNum).insert(typeId, value);

        // Insert index attribute values into the secondary index,
        // key is attribute value，value is pair (timestamp, rid)
        for (int i = 0; i < indexAttrNum; ++i) {
            secondaryIndexes.get(i).insert(attrValArray[i], value);
        }

        // primary index store timestamp and rid
        primaryIndex.insert(timestamp, rid);

        autoIndices++;
        return true;
    }

    @Override
    public boolean insertBatchRecord(String[] record) {
        return false;
    }

    @Override
    public int countQuery(EventPattern pattern, AbstractJoinStrategy join) {

        String[] seqVarNames = pattern.getSeqVarNames();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        final int patternLen = seqVarNames.length;

        // Note that the attribute type is considered the 0th attribute,
        // and the index index index is -1
        List<List<SelectivityPair>> alphas = new ArrayList<>(patternLen);
        for (int i = 0; i < patternLen; ++i) {
            List<SelectivityPair> selPairs = new ArrayList<>();
            String varName = seqVarNames[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            String curType = seqEventTypes[i];
            // view type as 0-th attribute, alpha = r_{s_i} / sumArrival
            double alpha0 = arrivals.get(curType) / sumArrival;
            selPairs.add(new SelectivityPair(-1, alpha0));
            // 计算属性选择率
            for(int j = 0; j < icList.size(); ++j){
                IndependentConstraint ic = icList.get(j);
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                long min = ic.getMinValue();
                long max = ic.getMaxValue();
                double sel = reservoir.selectivity(idx, min, max);
                selPairs.add(new SelectivityPair(j, sel));
            }

            alphas.add(selPairs);
        }

        // Choose one variable or two variables to generate replay intervals
        SingleVarChoice choice = optimalChoiceUsingSingleVar(alphas, pattern.getTau());
        SingleVarChoice[] choices = optimalChoiceUsingTwoVar(alphas, pattern.getTau());

        long startTime0 = System.nanoTime();
        ReplayIntervals replayIntervals;
        if(choice.minCost() < choices[0].minCost()){
            //System.out.println("choose one variable, id: " + choice.variableId());
            if(!debug){
                String output = String.format("%.3f", (choice.minCost()) / 1_000_000);
                System.out.println("predicated cost: " + output + "ms");
            }
            replayIntervals = getReplyIntervals(choice, pattern);
        }else{
            //System.out.println("choose two variable, id: " + choices[0].variableId() + ", " + choices[1].variableId());
            if(!debug){
                String output = String.format("%.3f", (choices[0].minCost()) / 1_000_000);
                System.out.println("predicated cost: " + output + "ms");
            }
            replayIntervals = getReplyIntervals(choices, pattern);
        }
        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0) / 1_000_000);
            System.out.println("filter cost: " + output + "ms");
        }

        // 字节记录桶
        List<List<byte[]>> buckets = getBucketsUsingIntervals(pattern, replayIntervals);

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
    public List<Tuple> tupleQuery(EventPattern p, AbstractJoinStrategy join) {
        return null;
    }

    @Override
    public void print() {

    }

    /**
     * Using one variable to generate replay intervals
     * @param alphas        variable selectivity
     * @param tau           time windows
     * @return              optimal one variable choice
     */
    public final SingleVarChoice optimalChoiceUsingSingleVar(List<List<SelectivityPair>> alphas, long tau){
        final int patternLen = alphas.size();
        double minCost = Double.MAX_VALUE;
        SingleVarChoice ans = null;
        for(int i = 0; i < patternLen; ++i){
            boolean headOrTail = (i == 0 || i == patternLen - 1);
            // Calculate for the variable v_i,  the minimum cost of index + replay interval
            SingleVarChoice curChoice = greedyUsingSingleVar(alphas.get(i), headOrTail, i, tau);
            if(curChoice.minCost() < minCost){
                minCost = curChoice.minCost();
                ans = curChoice;
            }
        }

        return ans;
    }

    /**
     * The optimal choice when generating replay intervals using a variable
     * @param selectivityPairs      selection rate of each predicate for each variable
     * @param headOrTail            the variable whether is first or last variable in pattern
     * @return                      optimal SingleVarChoice(varId + minCost + predicatePos)
     */
    public final SingleVarChoice greedyUsingSingleVar(List<SelectivityPair> selectivityPairs, boolean headOrTail, int varId, long tau){
        double minCost = Double.MAX_VALUE;
        // First, sort based on selection rate
        selectivityPairs.sort(Comparator.comparingDouble(SelectivityPair::selectivity));
        // determine the size of the time window
        long w = headOrTail ? tau : (tau << 1);


        double preIndexCost = 0;
        double prodAlpha = 1;
        boolean merge = false;
        double nIE = w * sumArrival;

        List<Integer> predicatePos = new ArrayList<>();
        double C_PageDivN_p = ConstCosts.C_PAGE / schema.getPageStoreRecordNum();
        // greedy choose optimal predicate
        for (SelectivityPair selectivityPair : selectivityPairs) {
            double alpha = selectivityPair.selectivity();
            prodAlpha *= alpha;
            double nIJ = autoIndices * alpha;
            if(nIJ == 0){
                nIJ = 1;
            }
            int needMerge = merge ? 1 : 0;
            // tree index searches from the root node to the leaf node, and the time is ignored here
            double curIndexCost = nIJ * (ConstCosts.C_TREE +
                    ConstCosts.C_SORT * Math.log(nIJ) / Math.log(2) + needMerge * ConstCosts.C_MERGE);
            // time for generating replay intervals and time for searching the skip list from the root node to the leaf node are ignored here
            double scanCost = prodAlpha * autoIndices * nIE * (ConstCosts.C_LIST + C_PageDivN_p);
            double indexCost = curIndexCost + preIndexCost;
            double sumCost = indexCost + scanCost;

            if (sumCost < minCost) {
                minCost = sumCost;
                predicatePos.add(selectivityPair.index());
                preIndexCost = indexCost;
                merge = true;
            } else {
                break;
            }
        }
        return new SingleVarChoice(varId, minCost, predicatePos);
    }

    /**
     * Using two variables to generate replay intervals
     * @param alphas        variable selectivity
     * @param tau           time windows
     * @return              optimal two variables choice
     */
    public final SingleVarChoice[] optimalChoiceUsingTwoVar(List<List<SelectivityPair>> alphas, long tau){
        final int patternLen = alphas.size();
        double minCost = Double.MAX_VALUE;
        SingleVarChoice[] ans = null;
        for(int i = 0; i < patternLen - 1; ++i){
            for(int j = i + 1; j < patternLen; ++j){
                // check selectivity here
                SingleVarChoice[] curChoice = greedyUsingTwoVar(alphas, i, j, tau);
                if(curChoice[0].minCost() < minCost){
                    ans = curChoice;
                    minCost = curChoice[0].minCost();
                }
            }
        }
        return ans;
    }

    /**
     * Select the predicate with the lowest selection rate among the two variables each time<br>
     * varId1 < varId2
     * @param alphas        variable selectivity
     * @param varId1        1-th variable id
     * @param varId2        2-th variable id
     * @param tau           time windows
     * @return              two variables choice
     */
    public final SingleVarChoice[] greedyUsingTwoVar(List<List<SelectivityPair>> alphas, int varId1, int varId2, long tau){
        final int patternLen = alphas.size();
        List<SelectivityPair> selectivityPairs1 = alphas.get(varId1);
        List<SelectivityPair> selectivityPairs2 = alphas.get(varId2);
        // First, sort based on selection rate
        selectivityPairs1.sort(Comparator.comparingDouble(SelectivityPair::selectivity));
        selectivityPairs2.sort(Comparator.comparingDouble(SelectivityPair::selectivity));
        // size1 and size2 must be greater equal 1
        int size1 = selectivityPairs1.size();
        int size2 = selectivityPairs2.size();

        double prodAlpha1 = 1, prodAlpha2 = 1;
        List<Integer> predicatePos1 = new ArrayList<>();
        List<Integer> predicatePos2 = new ArrayList<>();

        double C_PageDivN_p = ConstCosts.C_PAGE / schema.getPageStoreRecordNum();
        double expectedWindow = (varId1 == 0 || varId2 == patternLen - 1) ? tau : 1.5 * tau;
        double nIJE = expectedWindow * sumArrival;

        // to avoid null, it must choose an attribute for each variable
        SelectivityPair pair1 = selectivityPairs1.get(0);
        SelectivityPair pair2 = selectivityPairs2.get(0);
        double selectivity1 = pair1.selectivity();
        double selectivity2 = pair2.selectivity();

        predicatePos1.add(pair1.index());
        predicatePos2.add(pair2.index());

        prodAlpha1 = prodAlpha1 * selectivity1;
        prodAlpha2 = prodAlpha2 * selectivity2;
        double nIK1 = autoIndices * selectivity1;
        // avoid occur 0
        if(nIK1 == 0){
            nIK1 = 1;
        }
        double nIK2 = autoIndices * selectivity2;
        if(nIK2 == 0){
            nIK2  = 1;
        }

        double preIndexCost1 = nIK1 * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK1) / Math.log(2));
        double preIndexCost2 = nIK2 * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK2) / Math.log(2));
        double initialScanCost = prodAlpha1 * prodAlpha2 * autoIndices * nIJE * (ConstCosts.C_LIST + C_PageDivN_p);
        double minCost = preIndexCost1 + preIndexCost2 + initialScanCost;

        boolean earlyStop = false;
        int i = 1, j = 1;
        while(i < size1 && j < size2) {
            SelectivityPair selPair1 = selectivityPairs1.get(i);
            SelectivityPair selPair2 = selectivityPairs2.get(j);
            double sel1 = selPair1.selectivity();
            double sel2 = selPair2.selectivity();
            // choose min sel
            if(sel1 < sel2){
                prodAlpha1 = prodAlpha1 * sel1;
                double nIK = autoIndices * sel1;
                double curIndexCost1 = nIK * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK) / Math.log(2)
                        + ConstCosts.C_MERGE);
                double scanCost = prodAlpha1 * prodAlpha2 * autoIndices * nIJE * (ConstCosts.C_LIST + C_PageDivN_p);
                double indexCost1 = preIndexCost1 + curIndexCost1;
                double sumCost = indexCost1 + preIndexCost2 + scanCost;
                if(sumCost < minCost){
                    minCost = sumCost;
                    predicatePos1.add(selPair1.index());
                    preIndexCost1 = indexCost1;
                    ++i;
                }else{
                    earlyStop = true;
                    break;
                }
            }else{
                prodAlpha2 = prodAlpha2 * sel2;
                double nIK = autoIndices * sel2;
                double curIndexCost2 = nIK * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK) / Math.log(2)
                        + ConstCosts.C_MERGE);
                double scanCost = prodAlpha1 * prodAlpha2 * autoIndices * nIJE * (ConstCosts.C_LIST + C_PageDivN_p);
                double indexCost2 = preIndexCost2 + curIndexCost2;
                double sumCost = indexCost2 + preIndexCost1 + scanCost;
                if(sumCost < minCost){
                    minCost = sumCost;
                    predicatePos2.add(selPair2.index());
                    preIndexCost2 = indexCost2;
                    ++j;
                }else{
                    earlyStop = true;
                    break;
                }
            }
        }

        while(i < size1 && !earlyStop){
            SelectivityPair selPair1 = selectivityPairs1.get(i);
            double sel1 = selPair1.selectivity();
            prodAlpha1 = prodAlpha1 * sel1;
            double nIK = autoIndices * sel1;
            double curIndexCost1 = nIK * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK) / Math.log(2) + ConstCosts.C_MERGE);
            double scanCost = prodAlpha1 * prodAlpha2 * autoIndices * nIJE * (ConstCosts.C_LIST + C_PageDivN_p);
            double indexCost1 = preIndexCost1 + curIndexCost1;
            double sumCost = indexCost1 + preIndexCost2 + scanCost;
            if(sumCost < minCost){
                minCost = sumCost;
                predicatePos1.add(selPair1.index());
                preIndexCost1 = indexCost1;
                ++i;
            }else{
                break;
            }
        }

        while(j < size2 && !earlyStop){
            SelectivityPair selPair2 = selectivityPairs2.get(j);
            double sel2 = selPair2.selectivity();
            prodAlpha2 = prodAlpha2 * sel2;
            double nIK = autoIndices * sel2;
            double curIndexCost2 = nIK * (ConstCosts.C_TREE + ConstCosts.C_SORT * Math.log(nIK) / Math.log(2) + ConstCosts.C_MERGE);
            double scanCost = prodAlpha1 * prodAlpha2 * autoIndices * nIJE * (ConstCosts.C_LIST + C_PageDivN_p);
            double indexCost2 = preIndexCost2 + curIndexCost2;
            double sumCost = indexCost2 + preIndexCost1 + scanCost;
            if(sumCost < minCost){
                minCost = sumCost;
                predicatePos2.add(selPair2.index());
                preIndexCost2 = indexCost2;
                ++j;
            }else{
                break;
            }
        }

        return new SingleVarChoice[]{new SingleVarChoice(varId1, minCost, predicatePos1), new SingleVarChoice(varId2, minCost, predicatePos2)};
    }

    /**
     * Generate replay intervals based on single variable
     * @param singleVarChoice   SingleVarChoice
     * @param pattern           query pattern
     * @return                  replay intervals
     */
    public final ReplayIntervals getReplyIntervals(SingleVarChoice singleVarChoice, EventPattern pattern){
        List<Integer> predicatePos = singleVarChoice.predicatePos();
        int varId = singleVarChoice.variableId();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqVarNames.length;
        String varName = seqVarNames[varId];
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
        // using index to determine replay interval
        long startTime0 = System.nanoTime();
        List<IndexValuePair> finalAns = null;
        for(int pos : predicatePos){
            List<IndexValuePair> triples;
            if(pos == -1){
                String[] seqEventTypes = pattern.getSeqEventTypes();
                String curType = seqEventTypes[varId];
                long typeId = schema.getTypeId(curType);
                triples = secondaryIndexes.get(indexAttrNum).rangeQuery(typeId, typeId);
            }else{
                IndependentConstraint ic = icList.get(pos);
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                triples = secondaryIndexes.get(idx).rangeQuery(ic.getMinValue(), ic.getMaxValue());
            }

            if(triples  == null){
                finalAns = null;
                break;
            }else{
                // sort based on RecordID
                triples.sort((o1, o2) -> {
                    RID rid1 = o1.rid();
                    RID rid2 = o2.rid();
                    if(rid1.page() == rid2.page()){
                        return rid1.offset() - rid2.offset();
                    }else{
                        return rid1.page() - rid2.page();
                    }
                });
                if(finalAns == null){
                    // do not merge
                    finalAns = triples;
                }else{
                    finalAns = mergePairs(finalAns, triples);
                }
            }
        }
        long endTime0 = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0) / 1_000_000);
            System.out.println("index cost: " + output + "ms");
        }

        if(finalAns == null){
            return null;
        }else{
            long startTime1 = System.nanoTime();
            ReplayIntervals ans = new ReplayIntervals();
            long leftOffset, rightOffset;
            if(varId == 0){
                leftOffset = 0;
                rightOffset = pattern.getTau();
            } else if (varId == patternLen - 1) {
                leftOffset = -pattern.getTau();
                rightOffset = 0;
            }else{
                leftOffset = -pattern.getTau();
                rightOffset = pattern.getTau();
            }

            // generate replay intervals
            for(IndexValuePair triple : finalAns){
                long t = triple.timestamp();
                ans.insert(t + leftOffset, t + rightOffset);
            }
            long endTime1 = System.nanoTime();
            if(!debug){
                String output = String.format("%.3f", (endTime1 - startTime1 + 0.0) / 1_000_000);
                System.out.println("generate replay interval cost: " + output + "ms");
            }
            return ans;
        }
    }

    /**
     * Generate replay intervals based on two variables
     * @param singleVarChoices  two variables choice
     * @param pattern           query pattern
     * @return                  replay intervals
     */
    public final ReplayIntervals getReplyIntervals(SingleVarChoice[] singleVarChoices, EventPattern pattern){
        String[] seqVarNames = pattern.getSeqVarNames();
        final int patternLen = seqVarNames.length;
        long tau = pattern.getTau();

        long startTime0 = System.nanoTime();
        List<List<IndexValuePair>> twoVarTriples = new ArrayList<>(2);
        for(SingleVarChoice choice : singleVarChoices){
            // StringBuffer output = new StringBuffer(128)
            // output.append("varId: ").append(choice.variableId()).append(" minCost: ").append(choice.minCost())
            // output.append(" predicate pos: ")
            // for(int pos : choice.predicatePos()){
            //    output.append(pos).append(" ")
            // }
            // System.out.println(output)

            List<Integer> predicatePos = choice.predicatePos();
            int varId = choice.variableId();
            String varName = seqVarNames[varId];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<IndexValuePair> finalAns = null;
            for(int pos : predicatePos) {
                List<IndexValuePair> triples;
                if (pos == -1) {
                    String[] seqEventTypes = pattern.getSeqEventTypes();
                    String curType = seqEventTypes[varId];
                    long typeId = schema.getTypeId(curType);
                    triples = secondaryIndexes.get(indexAttrNum).rangeQuery(typeId, typeId);
                } else {
                    IndependentConstraint ic = icList.get(pos);
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    triples = secondaryIndexes.get(idx).rangeQuery(ic.getMinValue(), ic.getMaxValue());
                }
                if (triples == null) {
                    finalAns = null;
                    break;
                } else {
                    // sort based on RecordID
                    triples.sort((o1, o2) -> {
                        RID rid1 = o1.rid();
                        RID rid2 = o2.rid();
                        if(rid1.page() == rid2.page()){
                            return rid1.offset() - rid2.offset();
                        }else{
                            return rid1.page() - rid2.page();
                        }
                    });
                    if (finalAns == null) {
                        // do not merge
                        finalAns = triples;
                    } else {
                        finalAns = mergePairs(finalAns, triples);
                    }
                }
            }
            twoVarTriples.add(finalAns);
        }
        long endTime0 = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0) / 1_000_000);
            System.out.println("index cost: " + output + "ms");
        }

        long startTime1 = System.nanoTime();
        boolean containHead = false;
        boolean containTail = false;
        if(singleVarChoices[0].variableId() == 0){
            containHead = true;
        }
        if(singleVarChoices[1].variableId() == patternLen - 1){
            containTail = true;
        }

        // join get replay intervals
        List<long[]> timePairs = joinTimePair(twoVarTriples, containHead, tau);
        // replay interval
        ReplayIntervals replayIntervals = new ReplayIntervals();
        if(containHead && containTail){
            for(long[] timePair : timePairs){
                replayIntervals.insert(timePair[0], timePair[1]);
            }
        }else if(containHead){
            // only contain head [timePair[0], timePair[0] + tau]
            for(long[] timePair : timePairs){
                replayIntervals.insert(timePair[0], timePair[0] + tau);
            }
        }else if(containTail){
            // only contain tail [timePair[1] - tau, timePair[1]]
            for(long[] timePair : timePairs){
                replayIntervals.insert(timePair[1] - tau, timePair[1]);
            }
        }else{
            // do not contain head and tail [timePair[1] - tau, timePair[0] + tau]
            for(long[] timePair : timePairs){
                replayIntervals.insert(timePair[1] - tau, timePair[0] + tau);
            }
        }
        long endTime1 = System.nanoTime();
        if(!debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0) / 1_000_000);
            System.out.println("generate replay interval cost: " + output + "ms");
        }
        return replayIntervals;
    }

    /**
     * Intersection of two lists
     * @param list1 1-th list
     * @param list2 2-th list
     * @return Intersection of two lists
     */
    public final List<IndexValuePair> mergePairs(List<IndexValuePair> list1, List<IndexValuePair> list2){
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
     * Join the timestamps of two variables
     * @param twoVarTriples     triples about two variable
     * @param containHead       whether first variable is 0-th variable
     * @param tau               time windows
     * @return                  join timestamp list
     */
    public final List<long[]> joinTimePair(List<List<IndexValuePair>> twoVarTriples, boolean containHead, long tau){
        List<long[]> ans = new ArrayList<>(128);

        List<IndexValuePair> triples1 = twoVarTriples.get(0);
        List<IndexValuePair> triples2 = twoVarTriples.get(1);
        final int size1 = triples1.size();
        final int size2 = triples2.size();

        if(containHead){
            // triples1 joins triples2
            int startPos2 = 0;
            for(IndexValuePair triple : triples1){
                for(int j = startPos2; j < size2; ++j){
                    long leftTime = triple.timestamp();
                    long rightTime = triples2.get(j).timestamp();
                    if(leftTime > rightTime){
                        startPos2++;
                    }else if(rightTime - leftTime > tau){
                        break;
                    }else{
                        long[] timePair = {leftTime, rightTime};
                        ans.add(timePair);
                    }
                }
            }
        }else{
            // triples2 joins triples1
            int startPos1 = 0;
            for(IndexValuePair triple : triples2){
                for(int i = startPos1; i < size1; ++i){
                    long leftTime = triples1.get(i).timestamp();
                    long rightTime = triple.timestamp();
                    if(leftTime > rightTime){
                        break;
                    }else if(rightTime - leftTime > tau){
                        startPos1++;
                    }else{
                        long[] timePair = {leftTime, rightTime};
                        ans.add(timePair);
                    }
                }
            }
        }

        return ans;
    }

    /**
     * Filter records based on replay intervals, noting that replay intervals are not intersecting
     * @param pattern           query pattern
     * @param replayIntervals   query intervals
     * @return                  byte record buckets
     */
    public final List<List<byte[]>> getBucketsUsingIntervals(EventPattern pattern, ReplayIntervals replayIntervals){
        long startTime = System.nanoTime();
        String[] seqVarNames = pattern.getSeqVarNames();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        int patternLen = seqVarNames.length;

        List<List<byte[]>> buckets = new ArrayList<>(patternLen);
        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        if(replayIntervals == null){
            return buckets;
        }

        if(!replayIntervals.check()){
            System.out.println("replay intervals have bug.");
        }

        List<long[]> replayIntervalList = replayIntervals.getAllReplayIntervals();

        EventStore store = schema.getStore();
        int typeIdx = schema.getTypeIdx();

        // For each non-intersecting replay interval
        for (long[] curInterval : replayIntervalList) {
            // Find the record rid under this replay interval
            List<RID> ridList = primaryIndex.rangeQuery(curInterval[0], curInterval[1]);
            for (RID rid : ridList) {
                byte[] record = store.readByteRecord(rid);
                String curType = schema.getTypeFromBytesRecord(record, typeIdx);
                // scan to filter
                for (int j = 0; j < patternLen; ++j) {
                    // 首先就要事件类型相等，相等了看谓词是不是满足约束条件，如果满足了就把它放在第i个桶里面
                    if (curType.equals(seqEventTypes[j])) {
                        String varName = seqVarNames[j];
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                        boolean satisfy = true;
                        // Check if the independent predicate constraint is met
                        for (IndependentConstraint ic : icList) {
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // Obtain the corresponding column for storage based on the attribute name
                            int col = schema.getAttrNameIdx(name);
                            long value = schema.getValueFromBytesRecord(record, col);
                            if (value < min || value > max) {
                                satisfy = false;
                                break;
                            }
                        }
                        if (satisfy) {
                            buckets.get(j).add(record);
                        }
                    }
                }
            }
        }
        long endTime = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime - startTime + 0.0) / 1_000_000);
            System.out.println("scan cost: " + output + "ms");
        }

        return buckets;
    }
}

/*
function
optimalChoiceUsingTwoVar
greedyUsingTwoVar
joinTimePair
getReplyIntervals
 */



