package Method;

import ArgsConfig.JsonMap;
import Common.*;
import Condition.IndependentConstraint;
import FastModule.*;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * 新版fast 要存储的事件数量是 16 * 1024 性能会非常好
 * 实际上，32 * 1024会更好，但是考虑到这样做会导致驻留在缓存中的数据会非常多
 * key = event type + data partition id
 * 这个版本的FAST已经废弃
 */

public class DiscardFast extends Index{
    public static boolean debug = true;
    private int partitionRecordNum;                         // optimal record number store in a partition
    private List<Long> attrMinValues;                       // 被索引属性的最小值
    private List<Long> attrMaxRange;                        // 被索引属性的最大值
    HashMap <String, Integer> partitionTable;               // 分区id表
    HashMap<String, List<FastKey>> keyTable;                // Key Table
    HashMap<FastKey, FastValue> hashTable;                  // Hash Table
    HashMap<String, FastSingleBuffer> buffers;              // Buffer

    public DiscardFast(String indexName){
        super(indexName);
    }

    @Override
    public void initial() {
        // set optimal record number store in a partition
        partitionRecordNum = 16 * 1024;  //16 * 1024
        setMinMaxArray();

        partitionTable = new HashMap<>();
        keyTable = new HashMap<>();
        hashTable = new HashMap<>();
        buffers = new HashMap<>();
        // 设置各个事件的到达率
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);
        // sampling
        reservoir = new ReservoirSampling(indexAttrNum);
    }

    /**
     * set the min value array and max range array
     */
    public final void setMinMaxArray(){
        // must set min value
        attrMinValues = new ArrayList<>(indexAttrNum);
        attrMaxRange = new ArrayList<>(indexAttrNum);
        for(int i = 0; i < indexAttrNum; ++i){
            attrMinValues.add(0L);
            attrMaxRange.add(Long.MAX_VALUE >> 1);
        }

        String[] indexAttrNames = getIndexAttrNames();
        for(int i = 0; i < indexAttrNum; ++i){
            String name = indexAttrNames[i];
            int idx = schema.getAttrNameIdx(name);
            attrMinValues.set(i, schema.getIthAttrMinValue(idx));
            attrMaxRange.set(i, schema.getIthAttrMaxValue(idx) - attrMinValues.get(i));
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
            }else if(attrTypes[i].equals("INT") && indexAttrNameMap.containsKey(attrNames[i])){
                int idx = indexAttrNameMap.get(attrNames[i]);
                attrValArray[idx] = Long.parseLong(splits[i]);
            }else if(attrTypes[i].contains("FLOAT") && indexAttrNameMap.containsKey(attrNames[i])) {
                int idx = indexAttrNameMap.get(attrNames[i]);
                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                attrValArray[idx] = (long) (Float.parseFloat(splits[i]) * magnification);
            } else if(attrTypes[i].contains("DOUBLE") && indexAttrNameMap.containsKey(attrNames[i])){
                int idx = indexAttrNameMap.get(attrNames[i]);
                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                attrValArray[idx] = (long) (Double.parseDouble(splits[i]) * magnification);
            }else{
                throw new RuntimeException("Don't support this data type.");
            }
        }

        // 蓄水池存储的是没有变换过的值
        reservoir.sampling(attrValArray, autoIndices);

        // 由于RangeBitmap只能存储非负数整数，因此需要变换变换后的值是: y = x - min
        for(int i = 0; i < indexAttrNum; ++i){
            attrValArray[i] -= attrMinValues.get(i);
        }

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        FastTemporaryTriple quad = new FastTemporaryTriple(timestamp, rid, attrValArray);

        // if there are existing buffer, then insert the record to buffer
        if(buffers.containsKey(eventType)){
            FastSingleBuffer buffer = buffers.get(eventType);
            if(buffer.append(quad)){
                //if(debug){System.out.println("buffer of type '" + eventType + "' full, it will construct range bitmap");}
                FastKey key = buildIndex(eventType, buffer);
                buffer.clearBuffer();
                // 更新key table
                if(keyTable.containsKey(eventType)){
                    keyTable.get(eventType).add(key);
                }else{
                    List<FastKey> fastKeys = new ArrayList<>();
                    fastKeys.add(key);
                    keyTable.put(eventType, fastKeys);
                }
            }
        }else{
            FastSingleBuffer buffer = new FastSingleBuffer(indexAttrNum, partitionRecordNum);
            buffers.put(eventType, buffer);
            buffer.append(quad);
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
        // System.out.println("seq pattern query");
        List<List<byte[]>> buckets = greedyFindRecordUsingSel(pattern);
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
     * build index for a data partition
     * @param eventType     event type
     * @param buffer        record buffer
     */
    public FastKey buildIndex(String eventType, FastSingleBuffer buffer){
        if(!partitionTable.containsKey(eventType)){
            partitionTable.put(eventType, 0);
        }

        int typeId = schema.getTypeId(eventType);
        int partitionId = partitionTable.get(eventType);
        // update
        List<FastTemporaryTriple> triples = buffer.getTriples();
        final int recordNum = triples.size();
        long startTime = triples.get(0).timestamp();
        long endTime = triples.get(recordNum - 1).timestamp();

        List<Long> minValues = buffer.getMinValues();
        List<Long> maxValues = buffer.getMaxValues();

        FastKey fastKey = new FastKey(typeId, partitionId, startTime, endTime, minValues, maxValues);
        FastValue fastValue = new FastValue(indexAttrNum, recordNum);

        // debug...
        if(hashTable.containsKey(fastKey)){
            throw new RuntimeException("there existing same key");
        }
        fastValue.insertBatch(triples, indexAttrNum, attrMaxRange);
        hashTable.put(fastKey, fastValue);
        // update partitionId
        partitionTable.replace(eventType, partitionId + 1);
        return fastKey;
    }

    public final List<List<byte[]>> seqFindAllRecord(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        final int patternLen = seqEventTypes.length;

        //  merge then query
        List<IthRID> queryRIDList = null;
        for(int i = 0; i < patternLen; ++i){
            long startTime = System.nanoTime();
            String eventType = seqEventTypes[i];
            String varName = seqVarNames[i];
            // 得到这个变量的所有独立谓词约束
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<IthRID> curIthRID = getIthRIDList(eventType, icList, i);
            queryRIDList = mergeRID(queryRIDList, curIthRID);
            long endTime = System.nanoTime();
            if(debug){
                String output = String.format("%.3f", (endTime - startTime + 0.0)/1_000_000);
                System.out.println(i + "-th variable filter cost: " + output + "ms");
            }
        }

        return getRecordBuckets(queryRIDList, patternLen);
    }

    /**
     * @param pattern   query pattern
     * @return          byte record buckets that hold the query pattern
     */
    public final List<List<byte[]>> greedyFindAllRecord(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;

        // 计算选择率最低的那个变量
        double minSel = Double.MAX_VALUE;
        int minIdx = -1;

        double sumArrival = 0;
        for(double a : arrivals.values()){
            sumArrival += a;
        }

        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            // 得到这个变量的所有独立谓词约束
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            double sel = arrivals.get(seqEventTypes[i]) / sumArrival;
            for(IndependentConstraint ic : icList){
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
            }

            if(sel < minSel){
                minIdx = i;
                minSel = sel;
            }
        }

        // 先去找选择率最小的那个变量位置
        long startOffset;
        long endOffset;
        if(minIdx == 0){
            startOffset = 0;
            endOffset = pattern.getTau();
        }else if(minIdx == patternLen - 1){
            startOffset = -pattern.getTau();
            endOffset = 0;
        }else{
            startOffset = -pattern.getTau();
            endOffset = pattern.getTau();
        }

        List<IthRID> finalIthRIDList = new ArrayList<>();

        long startTime0 = System.nanoTime();
        final String type0 = seqEventTypes[minIdx];
        final String var0 = seqVarNames[minIdx];
        List<IndependentConstraint> icList0 = pattern.getICListUsingVarName(var0);
        List<RidTimePair> ridTimePairs = getRidTimePairs(type0, icList0);
        // generate ReplayInterval and RIDList
        ReplayIntervals replayIntervals = new ReplayIntervals();

        for(RidTimePair pair : ridTimePairs){
            long t = pair.timestamp();
            replayIntervals.insert(t + startOffset, t + endOffset);
            finalIthRIDList.add(new IthRID(pair.rid(), minIdx));
        }

        // 然后再去找其他的buckets
        for(int i = 0; i < patternLen; ++i){
            if(i != minIdx){
                String eventType = seqEventTypes[i];
                String varName = seqVarNames[i];
                // 得到这个变量的所有独立谓词约束
                List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                // 没有命中replay间隙的话 置成0
                List<IthRID> ithRIDList = getRIDList(eventType, icList, i, replayIntervals);
                finalIthRIDList = mergeRID(finalIthRIDList, ithRIDList);
            }
        }

        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0)/1_000_000);
            System.out.println(minIdx + "-th variable filter cost: " + output + "ms");
        }

        return getRecordBuckets(finalIthRIDList, patternLen);
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

        record SelectivityIndexPair(double sel, int varId){}
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);
        double sumArrival = 0;
        for(double a : arrivals.values()){
            sumArrival += a;
        }

        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            double sel = arrivals.get(seqEventTypes[i]) / sumArrival;
            for(IndependentConstraint ic : icList){
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
            }
            varSelList.add(new SelectivityIndexPair(sel, i));
        }
        // Sort in descending order based on selection rate
        varSelList.sort(Comparator.comparingDouble(SelectivityIndexPair::sel));

        //for(SelectivityIndexPair p : varSelList){System.out.println("sel: " + p.sel() + " varId: " + p.varId());}

        int minSelIdx = varSelList.get(0).varId();
        long startOffset, endOffset;
        if(minSelIdx == 0){
            startOffset = 0;
            endOffset = pattern.getTau();
        }else if(minSelIdx == patternLen - 1){
            startOffset = -pattern.getTau();
            endOffset = 0;
        }else{
            startOffset = -pattern.getTau();
            endOffset = pattern.getTau();
        }

        List<IthRID> finalIthRIDList = new ArrayList<>();

        long startTime0 = System.nanoTime();
        String type0 = seqEventTypes[minSelIdx];
        String var0 = seqVarNames[minSelIdx];
        List<IndependentConstraint> icList0 = pattern.getICListUsingVarName(var0);
        List<RidTimePair> ridTimePairs = getRidTimePairs(type0, icList0);
        // generate ReplayInterval and RIDList based on this variable
        ReplayIntervals replayIntervals = new ReplayIntervals();

        for(RidTimePair pair : ridTimePairs){
            long t = pair.timestamp();
            replayIntervals.insert(t + startOffset, t + endOffset);
            finalIthRIDList.add(new IthRID(pair.rid(), minSelIdx));
        }
        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0)/1_000_000);
            System.out.println(minSelIdx + "-th variable filter cost: " + output + "ms");
        }

        for(int k = 1; k < patternLen; ++k){
            int varId = varSelList.get(k).varId();
            long startTime = System.nanoTime();
            String eventType = seqEventTypes[varId];
            String varName = seqVarNames[varId];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<IthRID> ithRIDList = getRIDList(eventType, icList, varId, replayIntervals);
            // merge RIDList because it can avoid access duplicate pages
            finalIthRIDList = mergeRID(finalIthRIDList, ithRIDList);
            long endTime = System.nanoTime();
            if(debug){
                String output = String.format("%.3f", (endTime - startTime + 0.0) / 1_000_000);
                System.out.println(varId + "-th variable filter cost: " + output + "ms");
            }
        }

        return getRecordBuckets(finalIthRIDList, patternLen);
    }


    /**
     * 获取满足第ith个变量的独立谓词约束的rid列表
     * @param eventType     第ith变量的事件类型
     * @param icList        独立谓词约束列表
     * @param ith           position
     * @return              满足条件的RID列表
     */
    public final List<IthRID> getIthRIDList(String eventType, List<IndependentConstraint> icList, int ith){
        // 得到满足条件的RID列表
        List<IthRID> ans = new ArrayList<>();
        record ICQuad(int idx, int mark, long min, long max){}
        final int icNum = icList.size();
        List<ICQuad> icQuads = new ArrayList<>(icNum);

        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();
            // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
            switch (mark) {
                case 1 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue()));
                case 2 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx)));
                case 3 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx)));
            }
        }

        long startTime0 = System.nanoTime();
        // 先从索引中的数据中选出
        if(keyTable.containsKey(eventType)) {
            List<FastKey> keys = keyTable.get(eventType);
            for (FastKey key : keys) {
                boolean earlyBreak = false;
                // 第一步是根据判断查询的值是否落在FastKey的的最大最小值之间，如果是再查FastValue，否则直接退出
                for(ICQuad quad : icQuads){
                    int idx = quad.idx();
                    long min = quad.min();
                    long max = quad.max();
                    if(key.getIthIndexAttrMaxValue(idx) < min || key.getIthIndexAttrMinValue(idx) > max){
                        earlyBreak = true;
                        break;
                    }
                }

                if(!earlyBreak){
                    FastValue fastValue = hashTable.get(key);
                    // debug...
                    if (fastValue == null) {throw new RuntimeException("Don't exist fastKey.");}

                    RoaringBitmap rb = null;
                    // assuming that all attributes of the query have indexes
                    for(ICQuad quad : icQuads){
                        int idx = quad.idx();
                        int mark = quad.mark();
                        long min = quad.min();
                        long max = quad.max();

                        RoaringBitmap curRB = null;
                        // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                        switch (mark) {
                            case 1 -> curRB = fastValue.gteQuery(idx, min);
                            case 2 -> curRB = fastValue.lteQuery(idx, max);
                            case 3 -> curRB = fastValue.betweenQuery(idx, min, max);
                        }

                        if(curRB == null){
                            rb = null;
                            break;
                        }

                        if (rb == null) {
                            rb = curRB;
                        } else {
                            rb.and(curRB);
                        }
                    }

                    if (rb != null) {
                        // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                        List<IthRID> singleAns = fastValue.getRIDList(rb, ith);
                        ans.addAll(singleAns);
                    }
                }
            }
        }
        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0)/1_000_000);
            System.out.println("index cost: " + output + "ms");
        }

        long startTime1 = System.nanoTime();
        // 再从buffer中筛选数据
        FastSingleBuffer buffer = buffers.get(eventType);
        if(buffer != null){
            List<FastTemporaryTriple> triples = buffer.getTriples();

            for(FastTemporaryTriple triple : triples){
                boolean satisfy = true;
                long[] attrValues = triple.attrValues();
                for(ICQuad quad : icQuads) {
                    int idx = quad.idx();
                    long min = quad.min();
                    long max = quad.max();
                    if(attrValues[idx] < min || attrValues[idx] > max){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    ans.add(new IthRID(triple.rid(), ith));
                }
            }
        }

        long endTime1 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0)/1_000_000);
            System.out.println("buffer cost: " + output + "ms");
        }

        return ans;
    }

    /**
     * 获取第ith个变量的事件，并且使用replay intervals过滤
     * @param eventType         查询模式第ith个变量的事件类型
     * @param icList            独立谓词约束列表
     * @param ith               variable position
     * @param replayIntervals   replay intervals
     * @return                  ith rid list
     */
    public final List<IthRID> getRIDList(String eventType, List<IndependentConstraint> icList, int ith, ReplayIntervals replayIntervals){
        //long startTime = System.nanoTime();
        List<RidTimePair> pairs = getRidTimePairs(eventType, icList);
        //long endTime = System.nanoTime();if(debug){String output = String.format("%.3f", (endTime - startTime + 0.0)/1_000_000);System.out.println("get pair cost: " + output + "ms");}
        return usingReplayIntervalsFilter(pairs, ith, replayIntervals);
    }

    /**
     * 得到rid和timestamp对
     * @param eventType     事件类型
     * @param icList        独立谓词约束
     * @return              RidTimePair List
     */
    public List<RidTimePair> getRidTimePairs(String eventType, List<IndependentConstraint> icList){
        List<RidTimePair> pairs = new ArrayList<>();
        record ICQuad(int idx, int mark, long min, long max){}
        final int icNum = icList.size();
        List<ICQuad> icQuads = new ArrayList<>(icNum);
        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();
            // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
            switch (mark) {
                case 1 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue()));
                case 2 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx)));
                case 3 -> icQuads.add(new ICQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx)));
            }
        }

        long startTime0 = System.nanoTime();
        // getRidTimePairs from bitmap
        if(keyTable.containsKey(eventType)) {
            List<FastKey> keys = keyTable.get(eventType);
            for (FastKey key : keys) {
                boolean earlyBreak = false;
                // 第一步是根据判断查询的值是否落在FastKey的的最大最小值之间，如果是再查FastValue，否则直接退出
                for(ICQuad quad : icQuads){
                    int idx = quad.idx();
                    long min = quad.min();
                    long max = quad.max();
                    if(key.getIthIndexAttrMaxValue(idx) < min || key.getIthIndexAttrMinValue(idx) > max){
                        earlyBreak = true;
                        break;
                    }
                }

                if(!earlyBreak){
                    FastValue fastValue = hashTable.get(key);
                    // debug...
                    if (fastValue == null) {throw new RuntimeException("Don't exist fastKey.");}

                    RoaringBitmap rb = null;
                    // assuming that all attributes of the query have indexes
                    for(ICQuad quad : icQuads){
                        int idx = quad.idx();
                        int mark = quad.mark();
                        long min = quad.min();
                        long max = quad.max();

                        RoaringBitmap curRB = null;
                        // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                        switch (mark) {
                            case 1 -> curRB = fastValue.gteQuery(idx, min);
                            case 2 -> curRB = fastValue.lteQuery(idx, max);
                            case 3 -> curRB = fastValue.betweenQuery(idx, min, max);
                        }

                        if(curRB == null){
                            rb = null;
                            break;
                        }

                        if (rb == null) {
                            rb = curRB;
                        } else {
                            rb.and(curRB);
                        }
                    }

                    if (rb != null) {
                        // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                        List<RidTimePair> ridTimePairs = fastValue.getRidTimePairs(rb);
                        pairs.addAll(ridTimePairs);
                    }
                }
            }
        }
        long endTime0 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime0 - startTime0 + 0.0)/1_000_000);
            System.out.println("index cost: " + output + "ms");
        }

        long startTime1 = System.nanoTime();
        // getRidTimePairs from buffer
        FastSingleBuffer buffer = buffers.get(eventType);
        if(buffer != null){
            List<FastTemporaryTriple> triples = buffer.getTriples();
            for(FastTemporaryTriple triple : triples){
                boolean satisfy = true;
                long[] attrValues = triple.attrValues();
                for(ICQuad quad : icQuads) {
                    int idx = quad.idx();
                    long min = quad.min();
                    long max = quad.max();
                    if(attrValues[idx] < min || attrValues[idx] > max){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    pairs.add(new RidTimePair(triple.rid(), triple.timestamp()));
                }
            }
        }
        long endTime1 = System.nanoTime();
        if(debug){
            String output = String.format("%.3f", (endTime1 - startTime1 + 0.0)/1_000_000);
            System.out.println("buffer cost: " + output + "ms");
        }

        return pairs;
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
            System.out.println("disk cost: " + output + "ms");
        }
        return buckets;
    }

    /**
     * 使用回放间隙去过滤不在时间戳的范围内的元组
     * @param pairs             rid and timestamp pair
     * @param ith               var position
     * @param replayIntervals   replay intervals
     * @return                  ithRIDList
     */
    public final List<IthRID> usingReplayIntervalsFilter(List<RidTimePair> pairs, int ith, ReplayIntervals replayIntervals){
        //long startTime = System.nanoTime();
        List<IthRID> ans = new ArrayList<>();
        // pointer position rewind to 0
        replayIntervals.rewind();
        for(RidTimePair pair : pairs){
            long t = pair.timestamp();
            if(replayIntervals.include(t)){
                ans.add(new IthRID(pair.rid(), ith));
            }
        }
        //long endTime = System.nanoTime();if(debug){String output = String.format("%.3f", (endTime - startTime + 0.0)/1_000_000);System.out.println("replay filter cost: " + output + "ms");}
        return ans;
    }
}
