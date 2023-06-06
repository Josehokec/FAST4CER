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


    public List<List<Long>> getTimestampUsingIC(EventPattern pattern){
        long start = System.currentTimeMillis();
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;

        List<List<Long>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }


        String[] attrTypes = schema.getAttrTypes();
        int typeColNum = schema.getTypeIdx();
        int timeColNum = schema.getTimestampIdx();

        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;

            while ((line = b.readLine()) != null) {

                String[] record = line.split(",");
                String curType = record[typeColNum];


                long curTimestamp  = Long.parseLong(record[timeColNum]);

                for(int i = 0; i < patternLen; ++i){

                    if(curType.equals(seqEventTypes[i])){
                        String varName = seqVarNames[i];
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);

                        boolean satisfy = true;
                        for(IndependentConstraint ic : icList){
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();

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


    public List<List<String>> getRecordUsingIC(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;

        List<List<String>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        String[] attrTypes = schema.getAttrTypes();
        int typeColNum = schema.getTypeIdx();
        int timeColNum = schema.getTimestampIdx();


        HashMap<String, Integer> map = new HashMap<>();
        String firstTimestamp = null;
        String lastTimestamp = null;

        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;

            while ((line = b.readLine()) != null) {
                String[] record = line.split(",");

                String curType = record[typeColNum];
                map.put(curType,map.getOrDefault(curType,0)+1);

                if(firstTimestamp == null){
                    firstTimestamp = record[timeColNum];
                }
                lastTimestamp = record[timeColNum];

                for(int i = 0; i < patternLen; ++i){

                    if(curType.equals(seqEventTypes[i])){
                        String varName = seqVarNames[i];
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);

                        boolean satisfy = true;
                        for(IndependentConstraint ic : icList){
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();

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
