package Experiment;

import Common.*;
import Method.FullScanFromFile;
import Method.FullScanFromStore;
import Method.Index;
import JoinStrategy.GreedyJoin;
import JoinStrategy.OrderJoin;
import JoinStrategy.Tuple;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;


public class Experiment {
    public static boolean printFlag = false;
    public static int queryLoopTime = 3;

    public double average(long[] array){
        final int len = array.length;
        if(len <= 2){
            throw new RuntimeException("length must greater than 2.");
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        double sum = 0;
        for(long v : array){
            sum += v;
            if(min > v){
                min = v;
            }
            if(max < v){
                max = v;
            }
        }
        return (sum - min -max) / (array.length - 2) / 1000000;
    }

    /**
     * 实验1是用来验证FullScan算法性能
     * 全局扫描+Join
     */
    public void runExperiment0(String filePath, JSONArray jsonArray, int joinMethod, String schemaName) {
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        FullScanFromFile fullScanFromFile = new FullScanFromFile(filePath, schema);

        // 查询语句
        for(int i = 0; i < jsonArray.size(); ++i){
            EventPattern p = StatementParser.queryPattern(jsonArray.getString(i));
            String returnStr = p.getReturnStr();
            long[] queryTime = new long[queryLoopTime];
            for(int j = 0; j < queryLoopTime; ++j){
                long startQuery = System.nanoTime();
                if(returnStr.contains("COUNT")){
                    // (1) Order Join 和 (2) Greedy Join
                    int cnt;
                    switch (joinMethod) {
                        case 1 -> cnt = fullScanFromFile.countQuery(p, new OrderJoin());
                        case 2 -> cnt = fullScanFromFile.countQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            cnt = fullScanFromFile.countQuery(p, new GreedyJoin());
                        }
                    }
                }else{
                    List<Tuple> tuples;
                    switch (joinMethod) {
                        case 1 -> tuples = fullScanFromFile.tupleQuery(p, new OrderJoin());
                        case 2 -> tuples = fullScanFromFile.tupleQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            tuples = fullScanFromFile.tupleQuery(p, new GreedyJoin());
                        }
                    }

                    if(printFlag){
                        for(Tuple t : tuples){
                            System.out.println(t);
                        }
                    }
                    System.out.println("cnt: " + tuples.size());
                }
                long finishQuery = System.nanoTime();
                queryTime[j] = finishQuery - startQuery;
            }
            System.out.println(i+ "-th query pattern " + " time cost: " + String.format("%.2f", average(queryTime)) + "ms");
        }
    }

    /**
     * 实验1验证FAST算法性能
     */
    void runExperiment1(String filePath, JSONArray jsonArray, int joinMethod) {
        // 索引初始化
        String createIndexStr = "CREATE INDEX index_name1 USING FAST ON stock(open, volume)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();

        // 插入事件记录,构建索引
        buildIndex(filePath, index);

        // 进行批处理查询
        indexBatchQuery(index, jsonArray, joinMethod);

    }

    /**
     * 实验2验证BPlusTree算法性能
     */
    void runExperiment2(String filePath, JSONArray jsonArray, int joinMethod){
        // 索引初始化
        String createIndexStr = "CREATE INDEX index_name1 USING B_PLUS_TREE ON stock(open, volume)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();

        // 插入事件记录,构建索引
        buildIndex(filePath, index);

        // 进行批处理查询
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    /**
     * 实验3验证的是SkipList性能
     */
    void runExperiment3(String filePath, JSONArray jsonArray, int joinMethod){
        // 索引初始化
        String createIndexStr = "CREATE INDEX index_name1 USING Skip_List ON stock(open, volume)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();

        // 插入事件记录,构建索引
        buildIndex(filePath, index);

        // 进行批处理查询
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    /**
     * 实验4验证的是RTree性能
     */
    void runExperiment4(String filePath, JSONArray jsonArray, int joinMethod){
        // 索引初始化
        String createIndexStr = "CREATE INDEX index_name1 USING R_Tree ON stock(open, volume)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();

        // 插入事件记录,构建索引
        buildIndex(filePath, index);

        // 进行批处理查询
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    /**
     * 实验5验证state of the art性能
     */
    public void runExperiment5(String filePath, JSONArray jsonArray, int joinMethod){
        // 索引初始化
        String createIndexStr = "CREATE INDEX index_name1 USING State_Of_The_ART ON stock(open, volume)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();

        // 插入事件记录,构建索引
        buildIndex(filePath, index);

        // 进行批处理查询
        indexBatchQuery(index, jsonArray, joinMethod);
    }

    /**
     * 插入数据构建索引
     * @param filePath 文件路径
     * @param index 索引类型
     */
    public void buildIndex(String filePath, Index index){
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;
            // 把满足相关谓词约束的事件照出来并且放到对应的桶中
            while ((line = b.readLine()) != null) {
                index.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }

        //index.print();
    }

    /**
     * 针对索引的批处理查询
     * @param index 索引类型
     * @param jsonArray 查询的模式，用json保存
     * @param joinMethod 选择的join方法
     */
    public void indexBatchQuery(Index index, JSONArray jsonArray, int joinMethod){
        final int queryNum = jsonArray.size();
        for(int curLoop = 0; curLoop < queryLoopTime; ++curLoop){
            // 对于每个查询语句
            for(int i = 0; i < queryNum; ++i){
                String patternStr = jsonArray.getString(i);
                EventPattern p = StatementParser.queryPattern(patternStr);
                String returnStr = p.getReturnStr();

                long startQuery = System.nanoTime();
                if(returnStr.contains("COUNT")){
                    // (1) Order Join 和 (2) Greedy Join
                    int cnt;
                    switch (joinMethod) {
                        case 1 -> cnt = index.countQuery(p, new OrderJoin());
                        case 2 -> cnt = index.countQuery(p, new GreedyJoin());
                        default -> {
                            System.out.println("do not support this join method");
                            cnt = index.countQuery(p, new GreedyJoin());
                        }
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
                    // 如果要打印的话再打印
                    if(printFlag){
                        for(Tuple t : tuples){
                            System.out.println(t);
                        }
                    }
                }
                long finishQuery = System.nanoTime();
                String output = String.format("%.2f", (finishQuery - startQuery + 0.0) / 1_000_000);
                System.out.println(i+ "-th query pattern" + " time cost: " + output + "ms");
            }
        }
        // System.out.println(i+ "-th query pattern" + " time cost: " + String.format("%.2f", average(queryTime)) + "ms");
    }

    /**
     * 公平性实验
     */
    void runDebugExperiment(String filePath, JSONArray jsonArray, int joinMethod, String schemaName){
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        FullScanFromStore fullScan = new FullScanFromStore(schema);

        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;
            // 把满足相关谓词约束的事件照出来并且放到对应的桶中
            while ((line = b.readLine()) != null) {
                fullScan.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }

        // 查询语句
        final int queryNum = jsonArray.size();
        for(int curLoop = 0; curLoop < queryLoopTime; ++curLoop){

            for(int i = 0; i < queryNum; ++i) {
                EventPattern p = StatementParser.queryPattern(jsonArray.getString(i));
                String returnStr = p.getReturnStr();

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
                        for (Tuple t : tuples) {
                            System.out.println(t);
                        }
                    }

                }
                long finishQuery = System.nanoTime();
                String output = String.format("%.2f", (finishQuery - startQuery + 0.0)/1_000_000);
                System.out.println(i+ "-th query pattern" + " time cost: " + output + "ms");
            }
        }
    }

    /**
     * 各个实验是对应的方法
     */
    public static void main(String[] args)  {
        // 是不是需要打印结过
        Experiment.printFlag = true;
        Experiment e = new Experiment();

        // 初始化语句
        String[] initialStatements = {"CREATE TABLE stock (ticker TYPE, open FLOAT.2, volume INT, time TIMESTAMP)",
                "ALTER TABLE stock ADD CONSTRAINT ticker IN RANGE [1,5000]",
                "ALTER TABLE stock ADD CONSTRAINT open IN RANGE [0,1000]",
                "ALTER TABLE stock ADD CONSTRAINT volume IN RANGE [0,100000]"};
        for(String statement : initialStatements){
            System.out.println(statement);
            String str = StatementParser.convert(statement);
            String[] words = str.split(" ");
            if(words[0].equals("ALTER")){
                StatementParser.setAttrValueRange(str);
            } else if (words[0].equals("CREATE") && words[1].equals("TABLE")){
                StatementParser.createTable(str);
            }
        }

        // 测试文件名字
        String[] filenames = {"simplest_file.csv", "debug_file.csv", "test_file_1M.csv", "test_file_10M.csv"};
        String filename = filenames[3];
        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + filename;

        // 读取Json文件中的每个查询模式
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "Query" + File.separator + "test_stock.json";
        String jsonStr = JsonReader.getJson(jsonFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonStr);

        // 目前支持两种join   (1) Order Join 和 (2) Greedy Join
        int joinMethod = 1;
        //System.out.println("greedy join");
        String schemaName = "STOCK";
        e.runDebugExperiment(filePath, jsonArray, joinMethod, schemaName);
        //e.runExperiment2(filePath, jsonArray, joinMethod);

        /**
         String schemaName = "STOCK";
         e.runDebugExperiment(filePath, jsonArray, joinMethod, schemaName);
        e.runExperiment0(filePath, jsonArray, joinMethod, schemaName);
         */
    }
}
