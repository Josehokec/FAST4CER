package Method;

import BPlusTree.MemoryBPlusTree;
import Common.EventSchema;
import Common.IndexValuePair;
import Common.Metadata;
import Common.StatementParser;
import FastModule.RidTimePair;
import SkipList.SkipList;
import Store.EventStore;
import Store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.function.Consumer;

public class GetCostConst {
    public static  boolean printFlag = true;
    public static int loopTime = 6;
    private final int recordNum;
    private final int recordSize;
    SkipList<RID> skipList;
    MemoryBPlusTree bPlusTree;
    List<IndexValuePair> valueList;
    Random random;
    public GetCostConst(int recordNum, EventSchema schema) {
        this.recordNum = recordNum;
        skipList = new SkipList<RID>();
        bPlusTree = new MemoryBPlusTree();
        valueList = new ArrayList<>(recordNum);
        random = new Random();
        recordSize = schema.getStoreRecordSize();
    }

    public void initial(EventStore store){
        for(int i = 0; i < recordNum; i++){
            byte[] record = new byte[recordSize];
            RID rid = store.insertByteRecord(record);
            // key = i, value = <timestamp, rid>
            IndexValuePair pair = new IndexValuePair(i, rid);
            skipList.insert(i, rid);
            valueList.add(pair);
            long key = random.nextInt(100);          // [0, 100) -> 100 numbers
            bPlusTree.insert(key, pair);
        }
    }

    /**
     * shuffle the array then sort the array, get time cost, then calculate the sort cost const
     * @return  sort cost const
     */
    public double getSortCost(){
        List<Double> sortCostList = new ArrayList<>(loopTime);
        for(int i = 0; i < loopTime; ++i){
            // random shuffle the array
            Collections.shuffle(valueList);
            long startSortTime = System.nanoTime();
            valueList.sort((o1, o2) -> {
                RID rid1 = o1.rid();
                RID rid2 = o2.rid();
                if(rid1.page() == rid2.page()){
                    return rid1.offset() - rid2.offset();
                }else{
                    return rid1.page() - rid2.page();
                }
            });
            long endSortTime = System.nanoTime();
            double curCostSort = (endSortTime - startSortTime) / (recordNum * Math.log(recordNum) / Math.log(2));
            sortCostList.add(curCostSort);
        }

        return removeMinMaxFindAverage(sortCostList);
    }

    /**
     * calculate the BPlusTree access an element cost const
     * @return BPlusTree access a element average cost
     */
    public double getTreeCost(){
        // 想办法把这个调大
        // value range query selectivity: [0.08, 0.16, 0.32, 0.64]
        long[] minValues = {1, 11, 21, 31};
        long[] maxValues = {8, 26, 52, 94};

        final int rangeQueryTimes = minValues.length;

        List<Double> treeCostList = new ArrayList<>(loopTime);
        for(int i = 0; i < loopTime; ++i){
            List<Double> x = new ArrayList<>(rangeQueryTimes);
            List<Double> y = new ArrayList<>(rangeQueryTimes);
            for(int j = 0; j < rangeQueryTimes; ++j){
                long start = System.nanoTime();
                List<IndexValuePair> queryTriples = bPlusTree.rangeQuery(minValues[j], maxValues[j]);
                long end = System.nanoTime();
                long queryCost = end - start;
                int num = queryTriples.size();
                x.add(num + 0.0);
                y.add(queryCost + 0.0);
            }
            treeCostList.add(getOptimalSlope(x, y));
        }

        return removeMinMaxFindAverage(treeCostList);
    }

    /**
     * calculate the SkipList access an element cost const
     * @return SkipList access a element average cost
     */
    public double getListCost(){
        // 0.03125 0.0625 0.125 0.25
        long[] startTimestamps = { 50, 40, 30, 20};
        long[] endTimestamps = {(recordNum >> 5) + 50, (recordNum >> 4) + 40, (recordNum >> 3) + 30, (recordNum >> 2) + 20};
        final int timeQueryTimes = endTimestamps.length;

        List<Double> listCostList = new ArrayList<>(loopTime);
        for(int i = 0; i < loopTime; ++i){
            List<Double> x = new ArrayList<>(timeQueryTimes);
            List<Double> y = new ArrayList<>(timeQueryTimes);
            for(int j = 0; j < timeQueryTimes; ++j){
                long start = System.nanoTime();
                List<RID> queryList = skipList.rangeQuery(startTimestamps[j], endTimestamps[j]);
                long end = System.nanoTime();
                long queryCost = end - start;
                int num = queryList.size();
                x.add(num + 0.0);
                y.add(queryCost + 0.0);
            }
            listCostList.add(getOptimalSlope(x, y));
        }

        return removeMinMaxFindAverage(listCostList);
    }

    public double getMergeCost(){
        List<Double> mergeCostList = new ArrayList<>(loopTime);
        for(int loop = 0; loop < loopTime; ++loop){
            List<IndexValuePair> a = new ArrayList<>();
            for(int i = 0; i < recordNum; i = i + 10){
                a.add(valueList.get(i + random.nextInt(10)));
            }
            List<IndexValuePair> b = new ArrayList<>();
            for(int i = 0; i < recordNum; i = i + 20){
                b.add(valueList.get(i + random.nextInt(20)));
            }
            int size1 = a.size();
            int size2 = b.size();
            long start = System.nanoTime();
            merge(a, b);
            long end = System.nanoTime();
            double mergeCost = (end - start + 0.0) / (size1 + size2);
            mergeCostList.add(mergeCost);
        }

        return removeMinMaxFindAverage(mergeCostList);
    }

    public double getPageCost(EventStore store){
        int maxPageNum = store.getPageNum() - 1;
        if(maxPageNum > 256){
            maxPageNum = 256;
        }
        // test read a page cost
        long startReadPage = System.nanoTime();
        store.getMappedBuffer(maxPageNum >> 3);
        store.getMappedBuffer(maxPageNum >> 2);
        store.getMappedBuffer(maxPageNum >> 1);
        store.getMappedBuffer(maxPageNum);
        long endReadPage = System.nanoTime();

        return (endReadPage - startReadPage + 0.0) / 4;
    }

    /**
     * Intersection of two ordered value vector
     * @param list1     value vector1
     * @param list2     value vector2
     * @return          Intersection vector
     */
    public static List<IndexValuePair> merge(List<IndexValuePair> list1, List<IndexValuePair> list2){
        if(list1 == null || list2 == null){
            return null;
        }
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

    /**
     * remove max value and min value, then get average
     * @param x     value list
     * @return      average
     */
    public static double removeMinMaxFindAverage(List<Double> x){
        x.sort(Double::compareTo);
        final int len = x.size();
        if(len < 3){
            throw new RuntimeException("please increase loop times");
        }
        double sum = 0;
        for(int i = 1; i < len - 1; ++i){
            sum += x.get(i);
        }
        return sum / (len - 2);
    }

    /**
     * Using ordinary least squares to obtain the optimal slope
     * @param x     vector x
     * @param y     vector y
     * @return      slope
     */
    public static double getOptimalSlope(List<Double> x, List<Double> y){
        final int len = x.size();
        double xSum = 0;
        double xSquareSum = 0;
        for (double v : x) {
            xSum += v;
            xSquareSum += (v * v);
        }
        double xAverage = xSum / len;
        double numerator = 0;
        for(int i = 0; i < len; ++i){
            numerator += y.get(i) * (x.get(i) - xAverage);
        }
        double denominator = xSquareSum - xSum * xSum / len;
        return numerator / denominator;
    }

    public static List<Double> getBitmapCosts(EventSchema schema, int partitionRecordNum){
        List<Double> costs2 = new ArrayList<>(3);

        List<Long> timestamps = new ArrayList<>(partitionRecordNum);
        List<RID> rids = new ArrayList<>(partitionRecordNum);
        Random r = new Random();
        int recordSize = schema.getStoreRecordSize();

        RangeBitmap.Appender appender1 = RangeBitmap.appender(Long.MAX_VALUE >> 1);

        // generate partitionRecordNum records
        for(int i = 0; i < partitionRecordNum; i++){
            byte[] record = new byte[recordSize];
            appender1.add(r.nextInt(100));
            timestamps.add(i + 0L);
            rids.add(new RID(0, 0));
        }

        RangeBitmap rb = appender1.build();

        // range bitmap range query test
        int[] queryMinValues = {0, 20, 40, 60, 80, 90};
        int[] queryMaxValues = {50, 60, 70, 80, 90, 95};

        final int queryLen = queryMaxValues.length;

        // C_BMP
        List<Double> bmpCostList = new ArrayList<>(loopTime);
        for(int i = 0; i < loopTime; ++i){
            List<Double> queryCosts = new ArrayList<>(queryLen);
            long start = System.nanoTime();
            for(int j = 0; j < queryLen; ++j){
                rb.between(queryMinValues[j], queryMaxValues[j]);
            }
            long end = System.nanoTime();
            long queryCost = end - start;
            double averageCost = (queryCost + 0.0) / partitionRecordNum / queryLen;
            bmpCostList.add(averageCost);
        }
        double bmpCost = removeMinMaxFindAverage(bmpCostList);

        costs2.add(bmpCost);

        // C_AND
        List<Double> andCostList = new ArrayList<>(loopTime);
        for(int i = 0; i < loopTime; ++i){
            RoaringBitmap typeRange = new RoaringBitmap();
            typeRange.add(0, partitionRecordNum >> 1);

            List<Double> queryCosts = new ArrayList<>(queryLen);

            for(int j = 0; j < queryLen; ++j){
                RoaringBitmap queryRB1 = rb.between(queryMinValues[j], queryMaxValues[j]);
                long start = System.nanoTime();
                typeRange.and(queryRB1);
                long end = System.nanoTime();
                long queryCost = end - start;
                queryCosts.add((queryCost + 0.0) / (partitionRecordNum >> 1));
            }

            double sumCost = 0;
            for(double c : queryCosts){
                sumCost += c;
            }
            double average = sumCost / queryLen;
            andCostList.add(average);
        }
        double andCost = removeMinMaxFindAverage(andCostList);
        costs2.add(andCost);

        // C_GET
        List<Double> getCostList = new ArrayList<>(loopTime);
        for(int loop = 0; loop < loopTime; ++loop){
            List<Double> queryCosts = new ArrayList<>(queryLen);
            for(int j = 0; j < queryLen; ++j){
                RoaringBitmap queryRB = rb.between(queryMinValues[j], queryMaxValues[j]);
                long start = System.nanoTime();
                final int len = queryRB.getCardinality();
                List<RidTimePair> pairs = new ArrayList<>(len);
                queryRB.forEach((Consumer<? super Integer>) i ->{
                    long timestamp = timestamps.get(i);
                    RID rid = rids.get(i);
                    pairs.add(new RidTimePair(rid, timestamp));
                });
                long end = System.nanoTime();
                long queryCost = end - start;
                int num = queryRB.getCardinality();
                queryCosts.add((queryCost + 0.0) / num);
            }
            double sumCost = 0;
            for(double d : queryCosts){
                sumCost += d;
            }
            getCostList.add(sumCost / queryLen);
        }
        double getCost = removeMinMaxFindAverage(getCostList);
        costs2.add(getCost);

        return costs2;
    }

    public static List<Double> getSchemaTreeCosts(EventSchema schema){
        int recordSize = schema.getStoreRecordSize();
        EventStore store = new EventStore(schema.getSchemaName() + "_cost_initial", recordSize);
        // 10M records
        GetCostConst getCostConst = new GetCostConst(10_000_000, schema);
        getCostConst.initial(store);

        // C_SORT, C_TREE, C_LIST, C_MERGE, C_PAGE
        List<Double> costs = new ArrayList<>();
        double sortCost = getCostConst.getSortCost();
        costs.add(sortCost);
        double treeCost = getCostConst.getTreeCost();
        costs.add(treeCost);
        double listCost = getCostConst.getListCost();
        costs.add(listCost);
        double mergeCost = getCostConst.getMergeCost();
        costs.add(mergeCost);
        double pageCost = getCostConst.getPageCost(store);
        costs.add(pageCost);

        return costs;
    }

    public static List<Double> getSchemaTreeCosts(){
        String createTable = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)";
        String str = StatementParser.convert(createTable);
        StatementParser.createTable(str);
        EventSchema schema = Metadata.meta.getEventSchema("SYNTHETIC");

        int recordSize = schema.getStoreRecordSize();
        EventStore store = new EventStore("synthetic_cost_initial", recordSize);

        // 10M records
        GetCostConst getCostConst = new GetCostConst(10_000_000, schema);
        getCostConst.initial(store);

        // C_SORT, C_TREE, C_LIST, C_MERGE, C_PAGE
        List<Double> costs = new ArrayList<>();
        double sortCost = getCostConst.getSortCost();
        costs.add(sortCost);
        double treeCost = getCostConst.getTreeCost();
        costs.add(treeCost);
        double listCost = getCostConst.getListCost();
        costs.add(listCost);
        double mergeCost = getCostConst.getMergeCost();
        costs.add(mergeCost);
        double pageCost = getCostConst.getPageCost(store);
        costs.add(pageCost);

        if(printFlag){
            System.out.println("C_SORT: " + String.format("%.4f", sortCost) + "ns");
            System.out.println("C_TREE: " + String.format("%.4f", treeCost) + "ns");
            System.out.println("C_LIST: " + String.format("%.4f", listCost) + "ns");
            System.out.println("C_MERGE: " + String.format("%.4f", mergeCost) + "ns");
            System.out.println("C_PAGE: " + String.format("%.4f", pageCost) + "ns");
        }

        return costs;
    }

    public static List<Double> getBitmapConstCosts(int partitionRecordNum){
        //
        String createTable = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)";
        String str = StatementParser.convert(createTable);
        StatementParser.createTable(str);
        EventSchema schema = Metadata.meta.getEventSchema("SYNTHETIC");
        getBitmapCosts(schema, partitionRecordNum);
        getBitmapCosts(schema, partitionRecordNum);
        getBitmapCosts(schema, partitionRecordNum);
        List<Double> costs2 = getBitmapCosts(schema, partitionRecordNum);

        if(printFlag){
            System.out.print("C_BMP: " + String.format("%.4f", costs2.get(0)));
            System.out.print(" C_AND: " + String.format("%.4f", costs2.get(1)));
            System.out.println(" C_GET: " + String.format("%.4f", costs2.get(2)));
        }

        return costs2;
    }

    public static void main(String[] args){
        // IntervalScan method need these cost const, please run the class get results
        // SSD and Disk is different about C_PAGE
        GetCostConst.getSchemaTreeCosts();

        // fast cost (discard)
        /*
        // test bitmap cost
        int[] partitionRecordNums = {64 * 1024, 48 * 1024, 40 * 1024, 32 * 1024};
        final int cnt = partitionRecordNums.length;
        for(int i = 0; i < cnt; ++i){
            System.out.println("partitionRecordNum: " + (partitionRecordNums[i] / 1024) + "K");
            GetCostConst.getSyntheticFastCosts(partitionRecordNums[i]);
        }
         */
    }
}
