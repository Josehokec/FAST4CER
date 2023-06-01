package Experiment;

import Common.*;
import Condition.DependentConstraint;
import JoinStrategy.GreedyJoin;
import JoinStrategy.OrderJoin;
import JoinStrategy.Tuple;
import Method.FullScanFromStore;
import Method.Index;
import net.sf.json.JSONArray;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class SyntheticDatasetExperiment {
    public static boolean printFlag = true;
    //public static boolean debug = false;
    public static int queryLoopTime = 1;

    public void testFast(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX fast USING FAST ON synthetic(a1, a2, a3, a4)";
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
        String createIndexStr = "CREATE INDEX interval_scan USING INTERVAL_SCAN ON synthetic(a1, a2, a3, a4)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveIndexUsingRTree(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX rtree USING R_Tree ON synthetic(a1, a2, a3, a4)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveRTree(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX rtree USING R_Tree_Plus ON synthetic(a1, a2, a3, a4)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveIndexUsingBPlusTree(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX btree USING B_PLUS_TREE ON synthetic(a1, a2, a3, a4)";
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
        String createIndexStr = "CREATE INDEX btree USING B_PLUS_TREE_PLUS ON synthetic(a1, a2, a3, a4)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testNaiveIndexUsingSkipList(String filePath, JSONArray jsonArray, int joinMethod){
        // create index
        String createIndexStr = "CREATE INDEX skip_list USING Skip_List ON synthetic(a1, a2, a3, a4)";
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
        String createIndexStr = "CREATE INDEX skip_list USING SKIP_LIST_Plus ON synthetic(a1, a2, a3, a4)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        // insert record to index
        buildIndex(filePath, index);
        // pattern query
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    public void testFullScan(String filePath, JSONArray jsonArray, int joinMethod){
        String schemaName = "SYNTHETIC";
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
        long begin = System.currentTimeMillis();

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

        long end = System.currentTimeMillis();
        System.out.println("sum time: " + (end - begin) + "ms");
    }

    public void initial(){
        String[] initialStatements = {"CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)",
                "ALTER TABLE synthetic ADD CONSTRAINT type IN RANGE [0,100]",
                "ALTER TABLE synthetic ADD CONSTRAINT a1 IN RANGE [0,1012]",
                "ALTER TABLE synthetic ADD CONSTRAINT a2 IN RANGE [0,1012]",
                "ALTER TABLE synthetic ADD CONSTRAINT a3 IN RANGE [0,1012]",
                "ALTER TABLE synthetic ADD CONSTRAINT a4 IN RANGE [0,1012]"};

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
        SyntheticDatasetExperiment e = new SyntheticDatasetExperiment();
        // create table and add constraints
        e.initial();

        String[] filenames = {"synthetic_2M.csv", "synthetic_4M.csv", "synthetic_6M.csv", "synthetic_8M.csv", "synthetic_10M.csv"};
        // default dataset size is 10M
        String filename = filenames[4];
        System.out.println("dataset name: " + filename);

        String dir = System.getProperty("user.dir");
        String filePrefix = dir + File.separator + "src" + File.separator + "main" + File.separator;
        String filePath = filePrefix + "dataset" + File.separator + filename;

        // please change the json file: three_synthetic_query.json | synthetic_query.json
        String jsonFilePath = filePrefix + "java" + File.separator + "Query" + File.separator + "synthetic_query.json";
        String jsonStr = JsonReader.getJson(jsonFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonStr);

        // joinMethod = 1 -> Order Join | joinMethod = 2 -> Greedy Join (exist bug)
        int joinMethod = 1;

        // 1.testFullScan (baseline) 2.testFast (ours) 3.testIntervalScan (advanced) testNaiveRTree
        // 4.testNaiveIndexUsingRTree (naive1) 5.testNaiveIndexUsingBPlusTree (naive2) 6.testNaiveIndexUsingSkipList (naive3)

        //  testNaiveRTree 44418ms, fast 18007ms
        e.testNaiveSkipListPlus(filePath, jsonArray, joinMethod);
    }
}
