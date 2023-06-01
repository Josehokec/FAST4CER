package Method;

import ArgsConfig.JsonMap;
import Common.*;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;
import Store.EventStore;
import Store.RID;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.File;
import java.util.*;

/**
 * tsLeftLen是一个参数，有相关算法来确定这个参数
 * tsBitLen需要给定的
 * minArray 要索引的属性的最小值
 * maxRangeArray 要索引的属性的最大范围，这个是为了构造RangeBitmap
 * eventTypeTsLeftList: 根据事件类型，找到对应的TsLeft值
 * hashTable就是fast核心结构
 * withoutIndexDataMap存储的是等待索引的数据
 * 如果读取到到记录到时间戳tesLeft大于lastTsLeft
 * 那么需要构建Bitmap索引了
 */
public class DiscardFast2 extends Index {
    private int highBitTsLen;                                          // 最优的高时间戳比特值
    private int tsLen;                                                  // 时间戳长度
    private long preHighBitTs;                                          // 上一次插入记录的tsLeft值
    private long[] minArray;                                            // 被索引属性的最小值
    private long[] maxRangeArray;                                       // 被索引属性的最大值
    HashMap<String, List<Long>> keyTable;                               // Key Table
    HashMap<FastKeyV1, FastValueV1> hashTable;                              // Hash Table
    HashMap<String, List<FastValueQuad>> withoutIndexDataMap;           // Buffer

    /**
     * 默认时间戳用64bit存储的
     * @param indexName 创建的索引名字
     */
    public DiscardFast2(String indexName){
        super(indexName);
    }

    /**
     * 索引部分参数的初始化
     */
    @Override
    public void initial(){
        tsLen = 64;
        preHighBitTs = -1;
        keyTable = new HashMap<>();
        hashTable = new HashMap<>();
        withoutIndexDataMap = new HashMap<>();
        // 设置各个事件的到达率
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);
        // 计算最优的高时间戳值
        highBitTsLen = getOptimalTsLeft();
        reservoir = new ReservoirSampling(indexAttrNum);
        setMinMaxArray();
    }

    /**
     * 需要根据schema的一些统计信息来计算
     * 得到最优的tsLeftLen值
     * @return 最优的tsLEftLen值
     */
    public final int getOptimalTsLeft(){

        Collection<Double> values = arrivals.values();
        int m = arrivals.size();
        double sumArrival = 0;
        for (double value : values) {
            sumArrival += value;
        }

        // 计算存储事件类型需要多少字节
        int E ;
        if(m <= 0x0ff){
            E = 8;
        }else if(m <= 0x0ffff){
            E = 16;
        }else if(m <= 0x0ffffff){
            E = 24;
        }else{
            E = 32;
        }

        double minSpace = Double.MAX_VALUE;
        int optimalH = -1;
        for(int h = 0; h <= tsLen; h = h + 8){
            double curSpace = sumArrival * (tsLen - h) + ((double) m) * (E + h) / Math.pow(2, tsLen - h);
            if(curSpace < minSpace){
                minSpace = curSpace;
                optimalH = h;
            }
        }
        System.out.println("Optimal h is: " + optimalH);
        return optimalH;

    }

    public final void setMinMaxArray(){
        minArray = new long[indexAttrNum];
        maxRangeArray = new long[indexAttrNum];
        String[] indexAttrNames = getIndexAttrNames();
        for(int i = 0; i < indexAttrNames.length; ++i){
            String name = indexAttrNames[i];
            int idx = schema.getAttrNameIdx(name);
            minArray[i] = schema.getIthAttrMinValue(idx);
            maxRangeArray[i] = schema.getIthAttrMaxValue(idx) - minArray[i];
        }
    }

    /**
     * 注意RangeBitmap不能存储负数，值要进行变换的
     * 假设原始值是x，对应的最小值是min，则变换后的值是: y = x - min
     * 很重要的一个前提是按照顺序插入，数据是按照table声明那样存储的，比如
     * APPLE,0.49,191,1666956664
     * RID没有实现
     * @param record 插入的记录
     * @return 是否插入成功
     */
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
                System.out.println("Class FastIndex - Don't support this data type.");
            }
        }

        // 没转换值之前先蓄水池采样
        reservoir.sampling(attrValArray, autoIndices);

        // 由于RangeBitmap只能存储非负数整数，因此需要变换变换后的值是: y = x - min
        for(int i = 0; i < indexAttrNum; ++i){
            attrValArray[i] -= minArray[i];
        }
        // 得到ts-Right的值    debug: 1677652658000
        int tsRightLen = tsLen - highBitTsLen;
        long tsLeft = timestamp >> tsRightLen;
        byte[] tsRight = Converter.longToBytes(timestamp, tsRightLen);

        // 把字符串类型的记录转换为字节记录，然后存储到文件中
        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);

        FastValueQuad quad = new FastValueQuad(attrValArray, tsRight, rid, bytesRecord);
        // 需要把之前没有构建索引的数据去构造索引
        if(preHighBitTs == -1){
            preHighBitTs = tsLeft;
        }
        else if(tsLeft > preHighBitTs){
            buildIndex(preHighBitTs);
            // 构造完后清空map
            withoutIndexDataMap.clear();
            preHighBitTs = tsLeft;
        }
        // 否则这部分数据应该插入缓存withoutIndexDataMap中
        if(withoutIndexDataMap.containsKey(eventType)){
            withoutIndexDataMap.get(eventType).add(quad);
        }else{
            List<FastValueQuad> list = new ArrayList<>();
            list.add(quad);
            withoutIndexDataMap.put(eventType, list);
        }
        // 记录索引
        autoIndices++;
        return true;
    }

    /**
     * 不再是单个记录单个记录插入，而是一大批记录插入
     * 之所以不调用insertRecord是因为效率会低
     * @param records 多个事件记录
     * @return 是否操作成功
     */
    @Override
    public boolean insertBatchRecord(String[] records){
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();
        int tsRightLen = tsLen - highBitTsLen;

        for(String record : records){
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
                    System.out.println("Class FastIndex - Don't support this data type.");
                }
            }

            // 没转换值之前先蓄水池采样
            reservoir.sampling(attrValArray, autoIndices);

            // 由于RangeBitmap只能存储非负数整数，因此需要变换变换后的值是: y = x - min
            for(int i = 0; i < indexAttrNum; ++i){
                attrValArray[i] -= minArray[i];
            }
            // 得到ts-Right的值    debug: 1677652658000
            long tsLeft = timestamp >> tsRightLen;
            byte[] tsRight = Converter.longToBytes(timestamp, tsRightLen);

            // 把字符串类型的记录转换为字节记录，然后存储到文件中
            byte[] bytesRecord = schema.convertToBytes(splits);
            EventStore store = schema.getStore();
            RID rid = store.insertByteRecord(bytesRecord);

            FastValueQuad quad = new FastValueQuad(attrValArray, tsRight, rid, bytesRecord);
            // 需要把之前没有构建索引的数据去构造索引
            if(preHighBitTs == -1){
                preHighBitTs = tsLeft;
            }
            else if(tsLeft > preHighBitTs){
                buildIndex(preHighBitTs);
                // 构造完后清空map
                withoutIndexDataMap.clear();
                preHighBitTs = tsLeft;
            }
            // 否则这部分数据应该插入缓存withoutIndexDataMap中
            if(withoutIndexDataMap.containsKey(eventType)){
                withoutIndexDataMap.get(eventType).add(quad);
            }else{
                List<FastValueQuad> list = new ArrayList<>();
                list.add(quad);
                withoutIndexDataMap.put(eventType, list);
            }
            // 记录索引
            autoIndices++;
        }

        return true;
    }

    /**
     * 目前先处理最简单一种情况，skip till next match 而且没有依赖谓词
     * 查询算法：把满足每个谓词约束的事件找到放到桶里，然后丢到Join去计算
     * @param pattern 查询模式
     * @return 返回满足条件的元组
     */
    @Override
    public int countQuery(EventPattern pattern, AbstractJoinStrategy join){
        //List<List<byte[]>> buckets = greedyFindAllRecord(pattern);
        System.out.println("seq pattern query");
        List<List<byte[]>> buckets = seqFindAllRecord(pattern);
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
    public List<Tuple> tupleQuery(EventPattern p, AbstractJoinStrategy join){
        return null;
    }

    /**
     * 天然有序的
     * 对没有索引的数据构造range bitmap，后续可以去实现支持并发构建的功能
     * 这里使用了lambda表达式
     * withoutIndexDataMap的k是String类型的事件类型
     * v是三元组，存储时间戳低位+RID+索引属性值
     * @param tsLeft: 构建时tsLeft的值
     */
    public void buildIndex(long tsLeft){
        // k是String类型的事件类型，v是对应的索引属性quad列表
        // v is List<FastValueTripe>
        withoutIndexDataMap.forEach((k, v) ->{

            System.out.println("event type: " + k + " number: " + v.size());

            int typeId = schema.getTypeId(k);
            int typeBitLen = schema.getTypeIdLen();
            FastKeyV1 fastKey = new FastKeyV1(tsLeft, highBitTsLen, typeId, typeBitLen);

            if(keyTable.containsKey(k)){
                List<Long> tsLeftList = keyTable.get(k);
                tsLeftList.add(tsLeft);
            }else{
                List<Long> tsLeftList = new ArrayList<>();
                tsLeftList.add(tsLeft);
                keyTable.put(k, tsLeftList);
            }
            // hashTable一定不包含这个key的
            assert !hashTable.containsKey(fastKey) : "Class FastValue has bug";

            FastValueV1 fastValue = new FastValueV1(indexAttrNum);// here has some errors
            fastValue.insertBatch(v, maxRangeArray);

            hashTable.put(fastKey, fastValue);
        });

    }

    public final List<List<byte[]>> seqFindAllRecord(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;
        // bucket存的是记录的字节数组
        List<List<byte[]>> buckets = new ArrayList<>(patternLen);

        boolean oldMethod = false;

        if(oldMethod){
            for(int i = 0; i < patternLen; ++i){
                long start = System.currentTimeMillis();
                String eventType = seqEventTypes[i];
                String varName = seqVarNames[i];
                // 得到这个变量的所有独立谓词约束
                List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                List<byte[]> curList = getRecordList(eventType, icList);
                buckets.add(curList);
                long end = System.currentTimeMillis();
                System.out.println(i + "-th variable cost " + (end - start) + "ms");
            }
        }else{
            //  合并在一起查比较好
            List<IthRID> queryRIDList = null;
            for(int i = 0; i < patternLen; ++i){
                String eventType = seqEventTypes[i];
                String varName = seqVarNames[i];
                // 得到这个变量的所有独立谓词约束
                List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                List<IthRID> curIthRID = getRIDList(eventType, icList, i);
                queryRIDList = mergeRID(queryRIDList, curIthRID);
            }
            // 这里要加上buffer里面的数据
            buckets = getRecordBuckets(pattern, queryRIDList, patternLen);
        }

        return buckets;
    }

    /**
     * replay interval开销太大了
     * @param pattern 查询模式
     * @return 满足条件的字节记录
     */
    public final List<List<byte[]>> greedyFindAllRecord(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;
        // bucket存的是记录字节数组
        List<List<byte[]>> buckets = new ArrayList<>(patternLen);
        for(int i = 0; i < patternLen; ++i){
            buckets.add(null);
        }
        // 计算选择率最低的那个变量
        double minSel = Double.MAX_VALUE;
        int minIdx = -1;

        for(int i = 0; i < patternLen; ++i){
            String varName = seqVarNames[i];
            // 得到这个变量的所有独立谓词约束
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            double sel = arrivals.get(seqEventTypes[i]);
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

        Roaring64Bitmap highBitRB = new Roaring64Bitmap();
        //Roaring64Bitmap replayInterval = new Roaring64Bitmap();
        // 先去找选择率最小的那个变量
        int pos;
        if(minIdx == 0){
            pos = -1;
        }else if(minIdx == patternLen - 1){
            pos = 1;
        }else{
            pos = 0;
        }

        // debug
        long start = System.currentTimeMillis();
        //List<byte[]> minBuckets = getMinSelRecordList(seqEventTypes[minIdx], pattern.getICListUsingVarName(seqVarNames[minIdx]),
        //        highBitRB, replayInterval, pos, pattern.getTau());
        List<byte[]> minBuckets = getMinSelRecordList(seqEventTypes[minIdx], pattern.getICListUsingVarName(seqVarNames[minIdx]),
                highBitRB, pos, pattern.getTau());
        buckets.set(minIdx, minBuckets);
        long end = System.currentTimeMillis();
        System.out.println(minIdx + "-th variable cost " + (end - start) + "ms");

        // 然后再去找其他的buckets
        for(int i = 0; i < patternLen; ++i){
            if(i != minIdx){
                // 找到对应的记录，然后存到桶里面
                start = System.currentTimeMillis();
                List<byte[]> curBucket = getRecordList(seqEventTypes[i], pattern.getICListUsingVarName(seqVarNames[i]),
                        highBitRB);
                buckets.set(i, curBucket);
                end = System.currentTimeMillis();
                System.out.println(i + "-th variable cost " + (end - start) + "ms");
            }


        }

        return buckets;
    }


    /**
     * 最小的选择率先去查询，然后生成replayInterval的位图
     * @param eventType         事件类型
     * @param icList            独立谓词约束列表
     * @param highBitRB         高bit位的时间戳位图
     * @param replayInterval    回放间隙
     * @param pos               最小索引所在位置编号
     * @param tau               查询事件间隙
     * @return                  满足条件的字节记录
     */
    public final List<byte[]> getMinSelRecordList(String eventType, List<IndependentConstraint> icList,
                                            Roaring64Bitmap highBitRB, Roaring64Bitmap replayInterval, int pos, long tau){

        List<byte[]> ans = new ArrayList<>();
        int lowBitTimeLen = tsLen - highBitTsLen;
        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();

        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，然后生成key
            List<Long> highBitTimestamps = keyTable.get(eventType);
            for (long highBitTimestamp : highBitTimestamps) {
                // 生成fastKey
                FastKeyV1 fastKey = new FastKeyV1(highBitTimestamp, highBitTsLen, typeId, typeBitLen);
                // fastValue肯定不空
                FastValueV1 fastValue = hashTable.get(fastKey);
                if(fastValue == null){
                    throw new RuntimeException("Don't exist fastKey.");
                }

                RoaringBitmap finalRangeBitmap = null;
                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    // idx 表示的是索引属性名字对应的存储位置
                    int idx = -1;
                    if (indexAttrNameMap.containsKey(attrName)) {
                        idx = indexAttrNameMap.get(attrName);
                    } else {
                        System.out.println("This attribute do not have an index.");
                    }

                    // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                    //  查询的时候切记要进行变换
                    int mark = ic.hasMinMaxValue();
                    RoaringBitmap curRangeBitmap = null;
                    switch (mark) {
                        case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                        case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                        case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                        default -> System.out.print("Class FastIndex - this attribute does not have index.");
                    }
                    if(finalRangeBitmap == null) {
                        finalRangeBitmap = curRangeBitmap;
                    }else {
                        finalRangeBitmap.and(curRangeBitmap);
                    }
                }
                if(finalRangeBitmap != null){
                    // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                    highBitRB.add(highBitTimestamp);
                    List<byte[]> singleAns = fastValue.getBytesRecordUsingRB(finalRangeBitmap, schema.getStore(),
                            lowBitTimeLen, pos, highBitTimestamp, tau, highBitRB, replayInterval);
                    ans.addAll(singleAns);
                }
            }
        }
        // 如果没有索引的数据中也含eventType的话，这里就不用回放间隙去过滤了
        if(withoutIndexDataMap.containsKey(eventType)){
            List<byte[]> withoutIndexAns = new ArrayList<>();
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            for(FastValueQuad quad : quads){
                long[] attrValues = quad.getAttrValues();
                boolean satisfy = true;
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                    int idx = indexAttrNameMap.get(attrName);
                    // 减去最小值 attrValue是变换过的
                    if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                            attrValues[idx] < ic.getMinValue() - minArray[idx]){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    withoutIndexAns.add(quad.getBytesRecord());
                }
            }
            ans.addAll(withoutIndexAns);
        }
        return ans;
    }

    public final List<byte[]> getMinSelRecordList(String eventType, List<IndependentConstraint> icList,
                                                  Roaring64Bitmap highBitRB, int pos, long tau){

        List<byte[]> ans = new ArrayList<>();
        int lowBitTimeLen = tsLen - highBitTsLen;
        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();

        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，然后生成key
            List<Long> highBitTimestamps = keyTable.get(eventType);
            for (long highBitTimestamp : highBitTimestamps) {
                // 生成fastKey
                FastKeyV1 fastKey = new FastKeyV1(highBitTimestamp, highBitTsLen, typeId, typeBitLen);
                // fastValue肯定不空
                FastValueV1 fastValue = hashTable.get(fastKey);
                if(fastValue == null){
                    throw new RuntimeException("Don't exist fastKey.");
                }

                RoaringBitmap finalRangeBitmap = null;
                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    // idx 表示的是索引属性名字对应的存储位置
                    int idx = -1;
                    if (indexAttrNameMap.containsKey(attrName)) {
                        idx = indexAttrNameMap.get(attrName);
                    } else {
                        System.out.println("This attribute do not have an index.");
                    }

                    // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                    //  查询的时候切记要进行变换
                    int mark = ic.hasMinMaxValue();
                    RoaringBitmap curRangeBitmap = null;
                    switch (mark) {
                        case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                        case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                        case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                        default -> System.out.print("Class FastIndex - this attribute does not have index.");
                    }
                    if(finalRangeBitmap == null) {
                        finalRangeBitmap = curRangeBitmap;
                    }else {
                        finalRangeBitmap.and(curRangeBitmap);
                    }
                }
                if(finalRangeBitmap != null && finalRangeBitmap.getCardinality() != 0){
                    // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                    List<byte[]> singleAns = fastValue.getBytesRecordUsingRB(finalRangeBitmap, schema.getStore(),
                            lowBitTimeLen, pos, highBitTimestamp, tau, highBitRB);
                    ans.addAll(singleAns);
                }
            }
        }
        // 如果没有索引的数据中也含eventType的话，这里就不用回放间隙去过滤了
        if(withoutIndexDataMap.containsKey(eventType)){
            List<byte[]> withoutIndexAns = new ArrayList<>();
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            for(FastValueQuad quad : quads){
                long[] attrValues = quad.getAttrValues();
                boolean satisfy = true;
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                    int idx = indexAttrNameMap.get(attrName);
                    // 减去最小值 attrValue是变换过的
                    if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                            attrValues[idx] < ic.getMinValue() - minArray[idx]){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    withoutIndexAns.add(quad.getBytesRecord());
                }
            }
            ans.addAll(withoutIndexAns);
        }
        return ans;
    }

    /**
     * 找到对应的记录
     * @param eventType         事件类型
     * @param icList            独立谓词约束列表
     * @param highBitRB         高bit位的时间戳位图
     * @param replayInterval    回放间隙
     * @return                  满足条件的字节记录
     */
    public final List<byte[]> getRecordList(String eventType, List<IndependentConstraint> icList,
                                      Roaring64Bitmap highBitRB, Roaring64Bitmap replayInterval){

        List<byte[]> ans = new ArrayList<>();
        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();
        int rightBitLen = tsLen - highBitTsLen;
        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，得在highBitRB里面才能生成key
            List<Long> highBitTsList = keyTable.get(eventType);
            for (long highBitTs : highBitTsList) {
                if(highBitRB.contains(highBitTs)){
                    FastKeyV1 fastKey = new FastKeyV1(highBitTs, highBitTsLen, typeId, typeBitLen);
                    // fastValue肯定不空
                    FastValueV1 fastValue = hashTable.get(fastKey);
                    if(fastValue == null){
                        throw new RuntimeException("Don't exist fastKey.");
                    }
                    RoaringBitmap finalRangeBitmap = null;

                    for (IndependentConstraint ic : icList) {
                        String attrName = ic.getAttrName();
                        // idx 表示的是索引属性名字对应的存储位置
                        int idx = -1;
                        if (indexAttrNameMap.containsKey(attrName)) {
                            idx = indexAttrNameMap.get(attrName);
                        } else {
                            System.out.println("This attribute do not have an index.");
                        }

                        // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                        //  查询的时候切记要进行变换
                        int mark = ic.hasMinMaxValue();
                        RoaringBitmap curRangeBitmap = null;
                        switch (mark) {
                            case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                            case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                            case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                    ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                            default -> System.out.print("Class FastIndex - this attribute does not have index.");
                        }
                        if(finalRangeBitmap == null) {
                            finalRangeBitmap = curRangeBitmap;
                        }else {
                            finalRangeBitmap.and(curRangeBitmap);
                        }
                    }

                    if(finalRangeBitmap != null){
                        // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                        List<byte[]> singleAns =  fastValue.getBytesRecordUsingRB(finalRangeBitmap, schema.getStore(),
                                rightBitLen, highBitTs, replayInterval);
                        ans.addAll(singleAns);
                    }
                }
            }
        }

        // 如果没有索引的数据中也含eventType的话，这里就不用回放间隙去过滤了
        if(withoutIndexDataMap.containsKey(eventType)){
            List<byte[]> withoutIndexAns = new ArrayList<>();
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            for(FastValueQuad quad : quads){
                long[] attrValues = quad.getAttrValues();
                boolean satisfy = true;
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                    int idx = indexAttrNameMap.get(attrName);
                    // 减去最小值 attrValue是变换过的
                    if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                            attrValues[idx] < ic.getMinValue() - minArray[idx]){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    withoutIndexAns.add(quad.getBytesRecord());
                }
            }
            ans.addAll(withoutIndexAns);
        }
        return ans;
    }

    /**
     * 不用replay间隙得到记录
     * @param eventType     事件类型
     * @param icList        独立谓词约束列表
     * @param highBitRB     高时间戳位值
     * @return              字节记录
     */
    public final List<byte[]> getRecordList(String eventType, List<IndependentConstraint> icList,
                                            Roaring64Bitmap highBitRB){

        List<byte[]> ans = new ArrayList<>();
        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();
        int rightBitLen = tsLen - highBitTsLen;
        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，得在highBitRB里面才能生成key
            List<Long> highBitTsList = keyTable.get(eventType);
            for (long highBitTs : highBitTsList) {
                if(highBitRB.contains(highBitTs)){
                    FastKeyV1 fastKey = new FastKeyV1(highBitTs, highBitTsLen, typeId, typeBitLen);
                    // fastValue肯定不空
                    FastValueV1 fastValue = hashTable.get(fastKey);
                    if(fastValue == null){
                        throw new RuntimeException("Don't exist fastKey.");
                    }
                    RoaringBitmap finalRB = null;

                    for (IndependentConstraint ic : icList) {
                        String attrName = ic.getAttrName();
                        // idx 表示的是索引属性名字对应的存储位置
                        int idx = -1;
                        if (indexAttrNameMap.containsKey(attrName)) {
                            idx = indexAttrNameMap.get(attrName);
                        } else {
                            System.out.println("This attribute do not have an index.");
                        }

                        // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                        //  查询的时候切记要进行变换
                        int mark = ic.hasMinMaxValue();
                        RoaringBitmap curRangeBitmap = null;
                        switch (mark) {
                            case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                            case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                            case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                    ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                            default -> System.out.print("Class FastIndex - this attribute does not have index.");
                        }
                        if(finalRB == null) {
                            finalRB = curRangeBitmap;
                        }else {
                            finalRB.and(curRangeBitmap);
                        }
                    }

                    if(finalRB != null && finalRB.getCardinality() != 0){
                        // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                        List<byte[]> singleAns =  fastValue.getBytesRecordUsingRB(finalRB, schema.getStore());
                        ans.addAll(singleAns);
                    }
                }
            }
        }

        // 如果没有索引的数据中也含eventType的话，这里就不用回放间隙去过滤了
        if(withoutIndexDataMap.containsKey(eventType)){
            List<byte[]> withoutIndexAns = new ArrayList<>();
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            for(FastValueQuad quad : quads){
                long[] attrValues = quad.getAttrValues();
                boolean satisfy = true;
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                    int idx = indexAttrNameMap.get(attrName);
                    // 减去最小值 attrValue是变换过的
                    if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                            attrValues[idx] < ic.getMinValue() - minArray[idx]){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    withoutIndexAns.add(quad.getBytesRecord());
                }
            }
            ans.addAll(withoutIndexAns);
        }
        return ans;
    }

    /**
     * 这个是不用过滤条件的函数
     * @param eventType     事件类型
     * @param icList        独立谓词约束列表
     * @return              字节记录
     */
    public final List<byte[]> getRecordList(String eventType, List<IndependentConstraint> icList){
        List<byte[]> ans = new ArrayList<>();
        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();

        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，得在highBitRB里面才能生成key
            List<Long> highBitTsList = keyTable.get(eventType);
            for (long highBitTs : highBitTsList) {
                FastKeyV1 fastKey = new FastKeyV1(highBitTs, highBitTsLen, typeId, typeBitLen);
                // fastValue肯定不空
                FastValueV1 fastValue = hashTable.get(fastKey);
                if (fastValue == null) {
                    throw new RuntimeException("Don't exist fastKey.");
                }

                RoaringBitmap finalRB = null;

                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    // idx 表示的是索引属性名字对应的存储位置
                    int idx = -1;
                    if (indexAttrNameMap.containsKey(attrName)) {
                        idx = indexAttrNameMap.get(attrName);
                    } else {
                        System.out.println("This attribute do not have an index.");
                    }

                    // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                    //  查询的时候切记要进行变换
                    int mark = ic.hasMinMaxValue();
                    RoaringBitmap curRangeBitmap = null;
                    switch (mark) {
                        case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                        case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                        case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                        default -> System.out.print("Class FastIndex - this attribute does not have index.");
                    }
                    if (finalRB == null) {
                        finalRB = curRangeBitmap;
                    } else {
                        finalRB.and(curRangeBitmap);
                    }
                }

                if (finalRB != null && finalRB.getCardinality() != 0) {
                    // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                    List<byte[]> singleAns = fastValue.getBytesRecordUsingRB(finalRB, schema.getStore());
                    ans.addAll(singleAns);
                }
            }
        }
        // 如果没有索引的数据中也含eventType的话，这里就不用回放间隙去过滤了
        if(withoutIndexDataMap.containsKey(eventType)){
            List<byte[]> withoutIndexAns = new ArrayList<>();
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            for(FastValueQuad quad : quads){
                long[] attrValues = quad.getAttrValues();
                boolean satisfy = true;
                for(IndependentConstraint ic : icList){
                    String attrName = ic.getAttrName();
                    // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                    int idx = indexAttrNameMap.get(attrName);
                    // 减去最小值 attrValue是变换过的
                    if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                            attrValues[idx] < ic.getMinValue() - minArray[idx]){
                        satisfy = false;
                        break;
                    }
                }
                if(satisfy){
                    withoutIndexAns.add(quad.getBytesRecord());
                }
            }
            ans.addAll(withoutIndexAns);
        }
        return ans;
    }

    public final List<IthRID> getRIDList(String eventType, List<IndependentConstraint> icList, int ith){
        List<IthRID> ans = new ArrayList<>();

        int typeId = schema.getTypeId(eventType);
        int typeBitLen = schema.getTypeIdLen();
        System.out.println("ith: " + ith);
        if(keyTable.containsKey(eventType)) {
            // 遍历每个tsLeft，得在highBitRB里面才能生成key
            List<Long> highBitTsList = keyTable.get(eventType);
            for (long highBitTs : highBitTsList) {
                FastKeyV1 fastKey = new FastKeyV1(highBitTs, highBitTsLen, typeId, typeBitLen);
                // fastValue肯定不空
                FastValueV1 fastValue = hashTable.get(fastKey);
                if (fastValue == null) {
                    throw new RuntimeException("Don't exist fastKey.");
                }

                RoaringBitmap finalRB = null;

                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    // idx 表示的是索引属性名字对应的存储位置
                    int idx = -1;
                    if (indexAttrNameMap.containsKey(attrName)) {
                        idx = indexAttrNameMap.get(attrName);
                    } else {
                        System.out.println("This attribute do not have an index.");
                    }

                    // 没有最大值和最小值则返回0，只有最小值则返回1，只有最大值则返回2，有最大最小值则返回3
                    //  查询的时候切记要进行变换
                    int mark = ic.hasMinMaxValue();
                    RoaringBitmap curRangeBitmap = null;
                    switch (mark) {
                        case 1 -> curRangeBitmap = fastValue.gteQuery(idx, ic.getMinValue() - minArray[idx]);
                        case 2 -> curRangeBitmap = fastValue.lteQuery(idx, ic.getMaxValue() - minArray[idx]);
                        case 3 -> curRangeBitmap = fastValue.betweenQuery(idx,
                                ic.getMinValue() - minArray[idx], ic.getMaxValue() - minArray[idx]);
                        default -> System.out.print("Class FastIndex - this attribute does not have index.");
                    }
                    // 如果根本没有满足条件的记录的话 直接break就行了
                    if(curRangeBitmap == null){
                        finalRB = null;
                        break;
                    }

                    if (finalRB == null) {
                        finalRB = curRangeBitmap;
                    } else {
                        finalRB.and(curRangeBitmap);
                    }
                }

                if (finalRB != null) {
                    // 把所有的独立约束都处理完了然后再去读取记录，并且记录回放间隙以及对应的高位时间戳bitmap
                    List<IthRID> singleAns = fastValue.getRIDList(finalRB, ith);
                    ans.addAll(singleAns);
                }
            }
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

    public final List<List<byte[]>> getRecordBuckets(EventPattern pattern, List<IthRID> ithRIDList, int patternLen){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();

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

        // 加上withoutIndexDataMap里面的数据
        for(int i = 0; i < patternLen; ++i){
            String eventType = seqEventTypes[i];
            String varName = seqVarNames[i];

            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            List<FastValueQuad> quads = withoutIndexDataMap.get(eventType);

            if(quads != null){
                for(FastValueQuad quad : quads){
                    long[] attrValues = quad.getAttrValues();
                    boolean satisfy = true;
                    for(IndependentConstraint ic : icList){
                        String attrName = ic.getAttrName();
                        // 如果谓词给的不含索引，idx需要判断一下，这里偷懒没有加了
                        int idx = indexAttrNameMap.get(attrName);
                        // 减去最小值 attrValue是变换过的
                        if(attrValues[idx] > ic.getMaxValue() - minArray[idx] ||
                                attrValues[idx] < ic.getMinValue() - minArray[idx]){
                            satisfy = false;
                            break;
                        }
                    }
                    if(satisfy){
                        buckets.get(i).add(quad.getBytesRecord());
                    }
                }
            }
        }

        return buckets;
    }

    @Override
    public void print(){
        System.out.println("----------------Index Information----------------");
        System.out.println("Index name: '" + getIndexName() + "'" + " schema name: '" + schema.getSchemaName() + "'");
        System.out.println("Index attribute name:" );
        for(String indexAttrName : getIndexAttrNames()){
            System.out.print(" '" + indexAttrName + "'");
        }
        System.out.print("\n index attribute min value:");
        for (long l : minArray) {
            System.out.print(" " + l);
        }
        System.out.print("\n index attribute max range:");
        for (long l : maxRangeArray) {
            System.out.print(" " + l);
        }
        System.out.println("\ntsLeftLen: "+ highBitTsLen + " tsBitLen: " + tsLen + " lastTsLeft: " + preHighBitTs);

        System.out.println("eventTypeTsLeftList has:");
        keyTable.forEach((k, v) ->{
            System.out.print("Event type: " + k);
            System.out.print(" TsLeft list: ");
            for(Long tsLeft : v){
                System.out.print(" " + tsLeft);
            }
            System.out.println();
        });

        System.out.println("withoutIndexDataMap has:");
        withoutIndexDataMap.forEach((k, v) -> System.out.println(k));
    }
}
