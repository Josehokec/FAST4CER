package Experiment;

import Common.*;
import Condition.DependentConstraint;
import JoinStrategy.GreedyJoin;
import JoinStrategy.OrderJoin;
import JoinStrategy.Tuple;
import Method.FullScanFromStore;
import Method.Index;
import net.sf.json.JSONArray;
import com.carrotsearch.sizeof.RamUsageEstimator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class TradeDatasetExperiment {
    public static boolean printFlag = true;
    //public static boolean debug = false;
    public static int queryLoopTime = 1;

    public void testFast(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX fast USING FAST ON trade(stockID, volume, price)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testIntervalScan(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX interval_scan USING INTERVAL_SCAN ON trade(stockID, volume, price)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveRTreePlus(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX rtree USING R_Tree_Plus ON trade(stockID, volume, price)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveBPlusTreePlus(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX btree USING B_PLUS_TREE_Plus ON trade(stockID, volume, price)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveSkipListPlus(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX skip_list USING Skip_List_Plus ON trade(stockID, volume, price)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testFullScan(String filePath, JSONArray jsonArray, int joinMethod){
        String schemaName = "TRADE";
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        FullScanFromStore fullScan = new FullScanFromStore(schema);

        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;
            while ((line = b.readLine()) != null) {
                fullScan.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }

        // query
        final int queryNum = jsonArray.size();
        for(int curLoop = 0; curLoop < queryLoopTime; ++curLoop){
            for(int i = 0; i < queryNum; ++i) {
                if(i != 0){
                    System.out.println(i + "-th query start...");
                }
                EventPattern p = StatementParser.queryPattern(jsonArray.getString(i));
                //p.print();
                String returnStr = p.getReturnStr();
                List<DependentConstraint> dcList = p.getDcList();
                long startQuery = System.nanoTime();
                if (returnStr.contains("COUNT")) {
                    // (1) Order Join 和 (2) Greedy Join
                    int cnt;
                    switch (joinMethod) {
                        case 1 -> cnt = fullScan.countQuery(p, new OrderJoin());
                        case 2 -> cnt = fullScan.countQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            cnt = fullScan.countQuery(p, new GreedyJoin());
                        }
                    }
                    if(printFlag){
                        System.out.println("number of tuples: " + cnt);
                    }
                } else {
                    List<Tuple> tuples;
                    switch (joinMethod) {
                        case 1 -> tuples = fullScan.tupleQuery(p, new OrderJoin());
                        case 2 -> tuples = fullScan.tupleQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            tuples = fullScan.tupleQuery(p, new GreedyJoin());
                        }
                    }

                    if (printFlag) {
                        if(tuples.size() == 0){
                            System.out.println("null");
                        }else{
                            for (Tuple t : tuples) {
                                System.out.println(t);
                            }
                        }
                    }

                }
                long finishQuery = System.nanoTime();
                String output = String.format("%.2f", (finishQuery - startQuery + 0.0) / 1_000_000);
                // discard first query result
                if(i != 0){
                    System.out.println(i+ "-th query pattern" + " time cost: " + output + "ms");
                }
            }
        }
    }

    /**
     * insert all records to index
     * @param filePath      record file
     * @param index         index
     */
    public void buildIndex(String filePath, Index index){
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;
            while ((line = b.readLine()) != null) {
                index.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * use index for batch querying
     * @param index         index
     * @param jsonArray     patterns that need to be queried
     * @param joinMethod    join method
     */
    public void indexBatchQuery(Index index, JSONArray jsonArray, int joinMethod){
        final int queryNum = jsonArray.size();
        for(int curLoop = 0; curLoop < queryLoopTime; ++curLoop){
            // for each query pattern
            for(int i = 0; i < queryNum; ++i){
                if(i != 0){
                    System.out.println(i + "-th query start...");
                }
                String patternStr = jsonArray.getString(i);
                EventPattern p = StatementParser.queryPattern(patternStr);
                String returnStr = p.getReturnStr();

                long startQuery = System.nanoTime();
                if(returnStr.contains("COUNT")){
                    // (1) Order Join (2) Greedy Join
                    int cnt;
                    switch (joinMethod) {
                        case 1 -> cnt = index.countQuery(p, new OrderJoin());
                        case 2 -> cnt = index.countQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            cnt = index.countQuery(p, new GreedyJoin());
                        }
                    }
                    if(printFlag){
                        System.out.println("number of tuples: " + cnt);
                    }
                }else{
                    List<Tuple> tuples;
                    switch (joinMethod) {
                        case 1 -> tuples = index.tupleQuery(p, new OrderJoin());
                        case 2 -> tuples = index.tupleQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            tuples = index.tupleQuery(p, new GreedyJoin());
                        }
                    }
                    if(printFlag){
                        if(tuples.size() == 0){
                            System.out.println("null");
                        }else{
                            for (Tuple t : tuples) {
                                System.out.println(t);
                            }
                        }
                    }
                }
                long finishQuery = System.nanoTime();
                String output = String.format("%.2f", (finishQuery - startQuery + 0.0) / 1_000_000);
                // discard first query result
                if(i != 0){
                    System.out.println(i+ "-th query pattern" + " time cost: " + output + "ms");
                }
            }
        }
        // System.out.println(i+ "-th query pattern" + " time cost: " + String.format("%.2f", average(queryTime)) + "ms");
    }

    public void initial(){
        String[] initialStatements = {
                "CREATE TABLE trade (type TYPE, transactionID INT,  stockID INT, volume INT, price DOUBLE.3, time TIMESTAMP)",
                "ALTER TABLE trade ADD CONSTRAINT type IN RANGE [0,50]",
                "ALTER TABLE trade ADD CONSTRAINT stockID IN RANGE [0,50]",
                "ALTER TABLE trade ADD CONSTRAINT volume IN RANGE [0,200000]",
                "ALTER TABLE trade ADD CONSTRAINT price IN RANGE [0,200]"
        };

        // create schema and define attribute range
        for(String statement : initialStatements){
            String str = StatementParser.convert(statement);
            String[] words = str.split(" ");
            if(words[0].equals("ALTER")){
                StatementParser.setAttrValueRange(str);
            } else if (words[0].equals("CREATE") && words[1].equals("TABLE")){
                StatementParser.createTable(str);
            }
        }
    }

    public static void main(String[] args){
        // please first run testFullScan method to generate arrival rate json file
        Experiment.printFlag = true;
        TradeDatasetExperiment e = new TradeDatasetExperiment();
        // create table and add constraints
        e.initial();

        String filename = "trade.csv";
        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "dataset" + File.separator + filename;
        // single_trade_query.json |
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java" +
                File.separator + "Query" + File.separator + "new_test_trade_query.json";

        String jsonStr = JsonReader.getJson(jsonFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonStr);

        // joinMethod = 1 -> Order Join | joinMethod = 2 -> Greedy Join (exist bug)
        int joinMethod = 2;

        // 1.testFullScan (baseline) 2.testFast (ours) 3.testIntervalScan (advanced)
        // 4.testNaiveRTreePlus (naive1) 5.testNaiveBPlusTreePlus (naive2) 6.testNaiveSkipListPlus (naive3)
        e.testFast(filePath, jsonArray, joinMethod);
    }
}
