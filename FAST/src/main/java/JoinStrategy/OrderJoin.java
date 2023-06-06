package JoinStrategy;

import Common.Converter;
import Common.EventPattern;
import Common.EventSchema;
import Common.Metadata;
import Condition.DependentConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Join with the order of defined sequence in event patterns
 * record PartialResultWithTime(List<Long> timeList, List<String> matchList){}
 */
public class OrderJoin extends AbstractJoinStrategy {
    public static boolean debug = true;
    /**
     * count using skip till next match(follow by) without dependent constraint
     * @param pattern event pattern
     * @param buckets buckets
     * @return count(*)
     */
    @Override
    public int countUsingS2WithoutDC(EventPattern pattern, List<List<Long>> buckets) {
        int patternLen = buckets.size();

        boolean flag = false;
        System.out.print("bucket sizes: [");
        for (List<Long> bucket : buckets) {
            int size = bucket.size();
            System.out.print(" " + size);
            if (size == 0) {
                flag = true;
            }
        }
        System.out.println(" ]");

        if (flag) {
            return 0;
        }

        // Next, we need to perform the Join operation list to store the timestamp of the matching tuple
        List<long[]> fullMatchTimestamps = new ArrayList<>();

        long tau = pattern.getTau();
        // Pointer acceleration
        int[] startPos = new int[patternLen];

        for (int i = 0; i < buckets.get(0).size(); ++i) {
            // The first event in the event pattern, the i-th element in the bucket
            long preTime = buckets.get(0).get(i);
            long[] tupleTimestamps = new long[patternLen];
            tupleTimestamps[0] = preTime;

            long remainingTime = tau;
            boolean canMatch = true;

            for (int j = 1; j < patternLen; ++j) {
                long curTime = buckets.get(j).get(startPos[j]);
                // Ensure the establishment of the SEQ relationship
                while (curTime < preTime) {
                    if (startPos[j] < buckets.get(j).size() - 1) {
                        startPos[j]++;
                        curTime = buckets.get(j).get(startPos[j]);
                    } else {
                        break;
                    }
                }
                // Ensure the establishment of WITHIN relationship
                long delta = curTime - preTime;
                if (delta <= remainingTime && delta >= 0) {
                    tupleTimestamps[j] = curTime;
                    remainingTime -= delta;
                    preTime = curTime;
                } else {
                    canMatch = false;
                    break;
                }
            }

            if (canMatch) {
                fullMatchTimestamps.add(tupleTimestamps);
            }
        }
        // output information

        for (long[] ts : fullMatchTimestamps) {
            System.out.print("[");
            for (long t : ts) {
                System.out.print(" " + t);
            }
            System.out.println(" ]");
        }
        return fullMatchTimestamps.size();
    }


    @Override
    public int countUsingS2WithDC(EventPattern pattern, List<List<String>> buckets){
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<String> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
       if(debug){
           System.out.println(output);
       }

        if(earlyStop){
            return 0;
        }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<String> bucket0 = buckets.get(0);
        List<PartialResultWithTime> partialMatches = new ArrayList<>(bucket0.size());

        for(String s : bucket0){
            long timestamp = Long.parseLong(s.split(",")[timeIdx]);
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<String> partialMatch = new ArrayList<>(1);
            partialMatch.add(s);
            partialMatches.add(new PartialResultWithTime(timestamps, partialMatch));
        }
        // 用循环做Join
        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = joinWithRecord(pattern, partialMatches, buckets.get(i));
        }

        HashSet<Long> timeSet = new HashSet<>();

        int ans = 0;
        for(PartialResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                ans++;
                timeSet.add(timestamp);
            }
        }

        return ans;
    }

    @Override
    public List<Tuple> getTupleUsingS2WithRecord(EventPattern pattern, List<List<String>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<String> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        if(debug){
            System.out.println(output);
        }

        if(earlyStop){ return new ArrayList<>(); }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<String> bucket0 = buckets.get(0);
        List<PartialResultWithTime> partialMatches = new ArrayList<>(bucket0.size());

        for(String s : bucket0){
            long timestamp = Long.parseLong(s.split(",")[timeIdx]);
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<String> partialMatch = new ArrayList<>(1);
            partialMatch.add(s);
            partialMatches.add(new PartialResultWithTime(timestamps, partialMatch));
        }


        int len = buckets.size();
        for(int i = 1; i < len; ++i){
            partialMatches = joinWithRecord(pattern, partialMatches, buckets.get(i));
        }

        HashSet<Long> timeSet = new HashSet<>();
        List<Tuple> ans = new ArrayList<>(partialMatches.size());

        for(PartialResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                timeSet.add(timestamp);
                Tuple t = new Tuple(len);
                t.addAllEvents(tuple.matchList());
            }
        }

        return ans;
    }


    @Override
    public int countUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");

        System.out.println(output);
        if(earlyStop){
            return 0;
        }

        int patternLen = buckets.size();

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        for(int i = 1; i < patternLen; ++i){
            partialMatches = joinWithBytesRecord(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        HashSet<Long> timeSet = new HashSet<>();
        int ans = 0;

        for(PartialBytesResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                ans++;
                timeSet.add(timestamp);
            }
        }

        return ans;
    }

    @Override
    public List<Tuple> getTupleUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return new ArrayList<>(); }


        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());


        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        List<Tuple> ans = new ArrayList<>();

        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = joinWithBytesRecord(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        HashSet<Long> timeSet = new HashSet<>();
        for(PartialBytesResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                timeSet.add(timestamp);
                Tuple t = new Tuple(buckets.size());
                for(byte[] record : tuple.matchList()){
                    String event = schema.bytesToRecord(record);
                    t.addEvent(event);
                }
                ans.add(t);
            }
        }
        return ans;
    }

    @Override
    public int countUsingS3WithDC(EventPattern pattern, List<List<String>> buckets){
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<String> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return 0; }


        pattern.sortDC();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<String> bucket0 = buckets.get(0);
        List<PartialResultWithTime> partialMatches = new ArrayList<>(bucket0.size());


        for(String s : bucket0){
            long timestamp = Long.parseLong(s.split(",")[timeIdx]);
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<String> partialMatch = new ArrayList<>(1);
            partialMatch.add(s);
            partialMatches.add(new PartialResultWithTime(timestamps, partialMatch));
        }


        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = joinWithRecord(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    @Override
    public List<Tuple> getTupleUsingS3WithRecord(EventPattern pattern, List<List<String>> buckets) {
        boolean earlyStop = false;
        System.out.print("bucket sizes: [");
        for (List<String> bucket : buckets) {
            int buckSize = bucket.size();
            System.out.print(" " + buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        System.out.println(" ]");

        if(earlyStop){ return new ArrayList<>(); }


        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<String> bucket0 = buckets.get(0);
        List<PartialResultWithTime> partialMatches = new ArrayList<>(bucket0.size());


        for(String s : bucket0){
            long timestamp = Long.parseLong(s.split(",")[timeIdx]);
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<String> partialMatch = new ArrayList<>(1);
            partialMatch.add(s);
            partialMatches.add(new PartialResultWithTime(timestamps, partialMatch));
        }

        int len = buckets.size();
        for(int i = 1; i < len; ++i){
            partialMatches = joinWithRecord(pattern, partialMatches, buckets.get(i));
        }

        List<Tuple> ans = new ArrayList<>(partialMatches.size());

        for(PartialResultWithTime tuple : partialMatches){
            Tuple t = new Tuple(len);
            t.addAllEvents(tuple.matchList());
        }
        return ans;
    }

    @Override
    public int countUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return 0; }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = joinWithBytesRecord(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    @Override
    public List<Tuple> getTupleUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return new ArrayList<>(); }


        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());


        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }
        List<Tuple> ans = new ArrayList<>();

        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = joinWithBytesRecord(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        for(PartialBytesResultWithTime tuple : partialMatches){
            Tuple t = new Tuple(buckets.size());
            for(byte[] record : tuple.matchList()){
                String event = schema.bytesToRecord(record);
                t.addEvent(event);
            }
            ans.add(t);
        }
        return ans;
    }


    public final List<PartialResultWithTime> joinWithRecord(EventPattern pattern, List<PartialResultWithTime> partialMatches, List<String> bucket){
        List<PartialResultWithTime> ans = new ArrayList<>();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        String[] attrTypes = schema.getAttrTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int timeIdx = schema.getTimestampIdx();

        long tau = pattern.getTau();

        int len = partialMatches.get(0).timeList().size();

        List<DependentConstraint> dcList = pattern.getContainIthVarDCList(len);

        int curPtr = 0;
        for (PartialResultWithTime partialMatch : partialMatches) {

            int curBucketSize = bucket.size();
            for(int i = curPtr; i < curBucketSize; ++i){
                String curRecord = bucket.get(i);
                String[] curAttrValues = curRecord.split(",");
                long curTime = Long.parseLong(curAttrValues[timeIdx]);

                if(curTime < partialMatch.timeList().get(0)){
                    curPtr++;
                }else if(curTime - partialMatch.timeList().get(0) > tau){
                    break;
                }else if(curTime > partialMatch.timeList().get(len - 1)){
                    boolean satisfy = true;
                    if(dcList != null && dcList.size() != 0){
                        for(DependentConstraint dc : dcList){
                            String attrName = dc.getAttrName();

                            int idx = schema.getAttrNameIdx(attrName);
                            boolean isVarName1 = seqVarNames[len].equals(dc.getVarName1());
                            String cmpVarName = isVarName1 ? dc.getVarName2() : dc.getVarName1();
                            int cmpVarIdx = pattern.getVarNamePos(cmpVarName);

                            String cmpRecord = partialMatch.matchList().get(cmpVarIdx);
                            String[] cmpAttrValues = cmpRecord.split(",");

                            long curValue;
                            long cmpValue;
                            if (attrTypes[idx].equals("INT")) {
                                curValue = Integer.parseInt(curAttrValues[idx]);
                                cmpValue = Integer.parseInt(cmpAttrValues[idx]);
                            } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                                curValue = (long) (Double.parseDouble(curAttrValues[idx]) * magnification);
                                cmpValue = (long) (Double.parseDouble(cmpAttrValues[idx]) * magnification);
                            } else {
                                throw new RuntimeException("Wrong index position.");
                            }
                            boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                            if (!hold) {
                                satisfy = false;
                                break;
                            }
                        }
                    }

                    if (satisfy) {
                        List<Long> timeList = new ArrayList<>(partialMatch.timeList());
                        timeList.add(curTime);

                        List<String> matchList = new ArrayList<>(partialMatch.matchList());
                        matchList.add(curRecord);

                        ans.add(new PartialResultWithTime(timeList, matchList));
                    }
                }
            }
        }
        return ans;
    }

    /**
     * join -> byte array record
     * @param pattern query pattern
     * @param partialMatches previous partial results
     * @param bucket bucket
     * @return new partial results
     */
    public final List<PartialBytesResultWithTime> joinWithBytesRecord(EventPattern pattern, List<PartialBytesResultWithTime> partialMatches, List<byte[]> bucket){
        // ans
        List<PartialBytesResultWithTime> ans = new ArrayList<>();
        // load schema
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // Load the index corresponding to the timestamp of the attribute type array and sequence variable array
        String[] attrTypes = schema.getAttrTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int timeIdx = schema.getTimestampIdx();
        // tau is the maximum time constraint for query patterns
        long tau = pattern.getTau();
        // Number of partially matched matches that have already been matched
        int len = partialMatches.get(0).timeList().size();

        // dcList
        List<DependentConstraint> dcList = pattern.getContainIthVarDCList(len);

        int curPtr = 0;
        for (PartialBytesResultWithTime partialMatch : partialMatches) {
            // SEQ needs to use the timestamp of the previous record WITH needs to use the timestamp of the first record
            int curBucketSize = bucket.size();
            for (int i = curPtr; i < curBucketSize; ++i) {
                byte[] curRecord = bucket.get(i);
                long curTime = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));

                if (curTime < partialMatch.timeList().get(0)) {
                    curPtr++;
                } else if (curTime - partialMatch.timeList().get(0) > tau) {
                    // If the within condition is not met,
                    // the following ones will definitely not be met either
                    break;
                } else if (curTime >= partialMatch.timeList().get(len - 1)) {

                    byte[] lastRecord = partialMatch.matchList().get(len - 1);
                    boolean isEqual = Arrays.equals(curRecord, lastRecord);

                    if(!isEqual){
                        boolean satisfy = true;
                        if (dcList != null && dcList.size() != 0) {
                            for (DependentConstraint dc : dcList) {
                                String attrName = dc.getAttrName();
                                // Find the index corresponding to the attribute name,
                                // and then determine whether the type is passed into DC for comparison to meet the conditions
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = seqVarNames[len].equals(dc.getVarName1());
                                String cmpVarName = isVarName1 ? dc.getVarName2() : dc.getVarName1();
                                int cmpVarIdx = pattern.getVarNamePos(cmpVarName);

                                byte[] cmpRecord = partialMatch.matchList().get(cmpVarIdx);

                                long curValue;
                                long cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                    cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    // Because the magnification has already been increased during storage,
                                    // there is no need to increase the magnification here
                                    curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                    cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                } else {
                                    throw new RuntimeException("Wrong index position.");
                                }
                                boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                if (!hold) {
                                    satisfy = false;
                                    break;
                                }
                            }
                        }

                        if (satisfy) {
                            List<Long> timeList = new ArrayList<>(partialMatch.timeList());
                            timeList.add(curTime);

                            List<byte[]> matchList = new ArrayList<>(partialMatch.matchList());
                            matchList.add(curRecord);

                            ans.add(new PartialBytesResultWithTime(timeList, matchList));
                        }
                    }
                }
            }
        }

        return ans;
    }

}
