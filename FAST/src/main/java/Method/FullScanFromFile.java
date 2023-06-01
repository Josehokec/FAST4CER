package Method;

import ArgsConfig.JsonMap;
import Common.*;
import Condition.IndependentConstraint;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FullScanFromFile {
    String filePath;
    EventSchema schema;

    public FullScanFromFile(String filePath, EventSchema schema){
        this.filePath = filePath;
        this.schema = schema;
    }

    /**
     * 如果没有依赖谓词直接传时间就行了
     * 用独立谓词去过滤元组,得到满足查询模式的每个事件
     * @param pattern 查询模式
     * @return 满足谓词约束的事件戳（一个桶）
     */
    public int countQuery(EventPattern pattern, AbstractJoinStrategy join){
        return countQueryWithDC(pattern, join);
    }

    public List<Tuple> tupleQuery(EventPattern pattern, AbstractJoinStrategy join){
        MatchStrategy strategy = pattern.getStrategy();
        List<List<String>> buckets = getRecordUsingIC(pattern);
        if(strategy == MatchStrategy.SKIP_TILL_NEXT_MATCH){
            return join.getTupleUsingS2WithRecord(pattern, buckets);
        }else if(strategy == MatchStrategy.SKIP_TILL_ANY_MATCH){
            return join.getTupleUsingS3WithRecord(pattern, buckets);
        }else{
            System.out.println("do not support this match strategy");
            return new ArrayList<>();
        }
    }

    /**
     * 弃用的方法
     */
    public int countQueryWithoutDC(EventPattern pattern, AbstractJoinStrategy join){
        MatchStrategy strategy = pattern.getStrategy();
        if(strategy == MatchStrategy.SKIP_TILL_NEXT_MATCH){
            List<List<Long>> buckets = getTimestampUsingIC(pattern);
            return join.countUsingS2WithoutDC(pattern, buckets);
        }else if(strategy == MatchStrategy.SKIP_TILL_ANY_MATCH){
            List<List<String>> buckets = getRecordUsingIC(pattern);
            return join.countUsingS3WithDC(pattern, buckets);
        }else{
            System.out.println("do not support this match strategy");
            return 0;
        }
    }

    /**
     * 有依赖谓词的计数查询 DC是DependentConstraint的简写
     * 思路是先根据独立谓词约束过滤，然后交给Join策略来处理依赖谓词
     */
    public int countQueryWithDC(EventPattern pattern, AbstractJoinStrategy join){
        MatchStrategy strategy = pattern.getStrategy();
        List<List<String>> buckets = getRecordUsingIC(pattern);
        if(strategy == MatchStrategy.SKIP_TILL_NEXT_MATCH){
            return join.countUsingS2WithDC(pattern, buckets);
        }else if(strategy == MatchStrategy.SKIP_TILL_ANY_MATCH){
            return join.countUsingS3WithDC(pattern, buckets);
        }else{
            System.out.println("do not support this match strategy");
            return 0;
        }
    }

    /**
     * 根据依赖谓词去过滤
     * @param pattern 查询模式
     * @return 每个变量对应的时间戳列表
     */
    public List<List<Long>> getTimestampUsingIC(EventPattern pattern){
        long start = System.currentTimeMillis();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;

        List<List<Long>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        // 得到Type在哪一列，时间戳在哪一列
        String[] attrTypes = schema.getAttrTypes();
        int typeColNum = schema.getTypeIdx();
        int timeColNum = schema.getTimestampIdx();


        // 读文件操作 把满足相关谓词约束的事件到相关的桶中
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;

            // 把满足相关谓词约束的事件照出来并且放到对应的桶中
            while ((line = b.readLine()) != null) {
                // 读取每一条记录
                String[] record = line.split(",");
                String curType = record[typeColNum];
                // hashmap中key值不存在就添加键值对(key,1)，key存在则对应的value值加1:

                long curTimestamp  = Long.parseLong(record[timeColNum]);
                // 看当前记录满足模式中的哪一个
                for(int i = 0; i < patternLen; ++i){
                    // 首先就要事件类型相等，相等了看谓词是不是满足约束条件，如果满足了就把它放在第i个桶里面
                    if(curType.equals(seqEventTypes[i])){
                        String varName = seqVarNames[i];
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                        // 假设最开始满足谓词约束
                        boolean satisfy = true;
                        for(IndependentConstraint ic : icList){
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // 根据属性名字得到其存储时候对应的列
                            int col = schema.getAttrNameIdx(name);
                            String valueType = attrTypes[col];

                            if(valueType.equals("INT")){
                                long value = Integer.parseInt(record[col]);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }else if(valueType.contains("FLOAT")){
                                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(col));
                                long value = (long) (Float.parseFloat(record[col]) * magnification);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }else if(valueType.contains("DOUBLE")){
                                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(col));
                                long value = (long) (Float.parseFloat(record[col]) * magnification);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }
                        }
                        if(satisfy){
                            buckets.get(i).add(curTimestamp);
                        }
                    }
                }
            }
            b.close();
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("filter time: " + (end - start) + "ms");



        return buckets;
    }

    /**
     * 根据独立谓词来过滤掉不相关的记录
     * IC是IndependentConstraint的简写
     * @param pattern 查询模式
     * @return 满足事件模式各个变量的事件记录
     */
    public List<List<String>> getRecordUsingIC(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;
        // bucket存的是记录的字符串
        List<List<String>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        // 得到Type在哪一列，时间戳在哪一列
        String[] attrTypes = schema.getAttrTypes();
        int typeColNum = schema.getTypeIdx();
        int timeColNum = schema.getTimestampIdx();

        // 计算事件到达率
        HashMap<String, Integer> map = new HashMap<>();
        String firstTimestamp = null;
        String lastTimestamp = null;
        // 读文件操作 把满足相关谓词约束的事件到相关的桶中
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;

            // 把满足相关谓词约束的事件照出来并且放到对应的桶中
            while ((line = b.readLine()) != null) {
                // 读取每一条记录
                String[] record = line.split(",");

                // 看当前记录满足模式中的哪一个
                String curType = record[typeColNum];
                map.put(curType,map.getOrDefault(curType,0)+1);
                // 时间戳
                if(firstTimestamp == null){
                    firstTimestamp = record[timeColNum];
                }
                lastTimestamp = record[timeColNum];

                for(int i = 0; i < patternLen; ++i){
                    // 首先就要事件类型相等，相等了看谓词是不是满足约束条件，如果满足了就把它放在第i个桶里面
                    if(curType.equals(seqEventTypes[i])){
                        String varName = seqVarNames[i];
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                        // 假设最开始满足谓词约束
                        boolean satisfy = true;
                        for(IndependentConstraint ic : icList){
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // 根据属性名字得到其存储时候对应的列
                            int col = schema.getAttrNameIdx(name);
                            String valueType = attrTypes[col];

                            if(valueType.equals("INT")){
                                long value = Integer.parseInt(record[col]);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }else if(valueType.contains("FLOAT")){
                                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(col));
                                long value = (long) (Float.parseFloat(record[col]) * magnification);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }else if(valueType.contains("DOUBLE")){
                                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(col));
                                long value = (long) (Float.parseFloat(record[col]) * magnification);
                                if(value < min || value > max){
                                    satisfy = false;
                                    break;
                                }
                            }
                        }
                        if(satisfy){
                            buckets.get(i).add(line);
                        }
                    }
                }
            }
            b.close();
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 将事件到达率存储为Json文件
        HashMap<String, Double> arrivals = new HashMap<>(map.size());
        long span = Long.parseLong(lastTimestamp) - Long.parseLong(firstTimestamp);
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String key = entry.getKey();
            double value = ((double) entry.getValue()) / span;
            arrivals.put(key, value);
        }
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "ArgsConfig" + File.separator + schemaName + "_arrivals.json";
        JsonMap.arrivalMapToJson(arrivals, jsonFilePath);

        return buckets;
    }
}
