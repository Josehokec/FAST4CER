package JoinStrategy;

import Common.Converter;
import Common.EventPattern;
import Common.EventSchema;
import Common.Metadata;
import Condition.DependentConstraint;

import java.util.*;

/**
 * 根据桶的含有的事件数量排序，然后进行Join
 */
public class GreedyJoin extends AbstractJoinStrategy{
    @Override
    public int countUsingS2WithoutDC(EventPattern pattern, List<List<Long>> buckets) {
        return -1;
    }

    /**
     * 新版的处理依赖谓词的函数，很快，超级快
     * @param pattern   要查询的模型
     * @param buckets   满足独立谓词约束的事件
     * @return          满足元组的数量
     */
    @Override
    public int countUsingS2WithDC(EventPattern pattern, List<List<String>> buckets){
        return -1;
    }

    @Override
    public List<Tuple> getTupleUsingS2WithRecord(EventPattern pattern, List<List<String>> buckets) {
        return null;
    }

    /**
     * bucket store byte record
     * @param pattern 查询模式
     * @param buckets 满足独立谓词的用
     * @return count(*)
     */
    @Override
    public int countUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        // First, sort based on the number of events in the bucket
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // early flag
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            output.append(" ").append(bucketSize);
            pairs.add(new Pair(bucketSize, i));
            if(bucketSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if (earlyStop) { return 0; }
        // sort according to number of events
        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        String schemaName = pattern.getSchemaName();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        // Load the index corresponding to the timestamp of the attribute type array and sequence variable array
        int timeIdx = schema.getTimestampIdx();

        // greedy choose
        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);

            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        // start join
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        HashSet<Long> timeSet = new HashSet<>();
        int ans = 0;
        // 最终的答案 用时间戳可能会更快
        for(PartialBytesResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                ans++;
                timeSet.add(timestamp);
            }
        }

        return ans;
    }

    /**
     * @param pattern   要查询的模型
     * @param buckets   满足独立谓词约束的事件
     * @return          满足条件的元组
     */
    @Override
    public List<Tuple> getTupleUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        // 首先根据桶的元组数量进行排序
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // 是否需要早停
        boolean flag = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            output.append(" ").append(bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        output.append(" ]");
        System.out.println(output);

        if (flag) { return new ArrayList<>(); }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // 加载属性类型数组和顺序变量数组已经时间戳对应的索引
        int timeIdx = schema.getTimestampIdx();

        // 把第一个事件加入进去
        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);

            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        List<Tuple> ans = new ArrayList<>();
        // 开始进行join操作
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        HashSet<Long> timeSet = new HashSet<>();

        for(PartialBytesResultWithTime tuple : partialMatches){
            long timestamp = tuple.timeList().get(0);
            if(!timeSet.contains(timestamp)){
                timeSet.add(timestamp);
                Tuple t = new Tuple(patternLen);
                for(byte[] record : tuple.matchList()){
                    String eventRecord = schema.bytesToRecord(record);
                    t.addEvent(eventRecord);
                }
                ans.add(t);
            }
        }
        return ans;
    }

    @Override
    public int countUsingS3WithDC(EventPattern pattern, List<List<String>> buckets){
        return -1;
    }

    @Override
    public List<Tuple> getTupleUsingS3WithRecord(EventPattern pattern, List<List<String>> buckets) {
        return null;
    }

    @Override
    public int countUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        // 首先根据桶的元组数量进行排序
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // 是否需要早停
        boolean flag = false;
        System.out.print("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            System.out.print(" " + bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        System.out.println(" ]");

        if (flag) { return 0; }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        String schemaName = pattern.getSchemaName();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        // 加载属性类型数组和顺序变量数组已经时间戳对应的索引
        int timeIdx = schema.getTimestampIdx();

        // 把第一个事件加入进去
        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        // 开始进行join操作
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            if(partialMatches.size() == 0){
                return 0;
            }
            // 更新marks,保证marks是有序的
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    /**
     * @param pattern 查询的模式
     * @param buckets 记录类型是字节数组类型的
     * @return 元组
     */
    @Override
    public List<Tuple> getTupleUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        // 首先根据桶的元组数量进行排序
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // 是否需要早停
        boolean flag = false;
        System.out.print("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            System.out.print(" " + bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        System.out.println(" ]");

        if (flag) { return new ArrayList<>(); }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // 加载属性类型数组和顺序变量数组已经时间戳对应的索引
        int timeIdx = schema.getTimestampIdx();

        // 把第一个事件加入进去
        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialBytesResultWithTime> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);
            partialMatches.add(new PartialBytesResultWithTime(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        List<Tuple> ans = new ArrayList<>();
        // 开始进行join操作
        for(int i = 1; i < patternLen; ++i){

            System.out.print("marks:");
            for(int m : marks){
                System.out.print(" " + m);
            }

            int curIndex = pairs.get(i).index();
            System.out.println(" curIndex: " + curIndex);
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            if(partialMatches.size() == 0){
                return ans;
            }
            // 保证marks是有序的
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        for(PartialBytesResultWithTime tuple : partialMatches){
            Tuple t = new Tuple(patternLen);
            for(byte[] record : tuple.matchList()){
                String eventRecord = schema.bytesToRecord(record);
                t.addEvent(eventRecord);

            }
            ans.add(t);
        }
        return ans;
    }

    /**
     * 新版的时间戳join策略
     * @param partialMatches 以前的部分匹配
     * @param marks 标记已经join的变量编号
     * @param bucket 要被join的变量事件时间戳
     * @param k 变量编号
     * @param tau 时间窗口
     * @return 部分匹配
     */
    public final List<List<Long>> joinWithTimestamp(List<List<Long>> partialMatches, List<Integer> marks,
                                                   List<Long> bucket, int k, long tau){
        int pos = -1;
        int len = marks.size();
        for(int i = 0; i < len; ++i){
            if(k < marks.get(i)){
                pos = i;
            }
        }
        if(pos == -1){
            pos = marks.size();
        }

        List<List<Long>> ans = new ArrayList<>();
        // 到时候会用这个加速，之所以能加速是因为事件时间有序
        int curPtr = 0;

        // [0, pos) k [pos, len)
        for(List<Long> partialMatch : partialMatches){

            for(int i = curPtr; i< bucket.size(); ++i) {
                long curTime = bucket.get(i);
                if (pos == 0) {
                    // SEQ检查 说明要被join的元素是第一个 如果这个时间戳大于部分匹配第一个时间戳 直接break
                    if (curTime > partialMatch.get(0)) {
                        break;
                    } else if (partialMatch.get(0) - curTime > tau) {
                        // 超过tau了， 更新指针位置，避免多次读取
                        curPtr++;
                    } else if (partialMatch.get(len - 1) - curTime <= tau) {
                        // 满足了SEQ关系，接下来检查是不是满足Within关系，如果满足依赖谓词约束那就加进来
                        List<Long> timeList = new ArrayList<>(len + 1);
                        timeList.add(curTime);
                        timeList.addAll(partialMatch);
                        ans.add(timeList);
                    }
                } else if (pos == len) {
                    if (curTime < partialMatch.get(0)) {
                        // 更新指针位置
                        curPtr++;
                    } else if (curTime - partialMatch.get(0) > tau) {
                        break;
                    } else if (curTime >= partialMatch.get(len - 1)) {
                        // 此时满足了Within和SEQ关系，开始进行依赖谓词约束检查 如果满足依赖谓词约束那就加进来
                        List<Long> timeList = new ArrayList<>(len + 1);
                        timeList.addAll(partialMatch);
                        timeList.add(curTime);
                        ans.add(timeList);
                    }
                }else{
                    // [0, pos) k [pos, len)
                    if(curTime < partialMatch.get(0)){
                        // 更新指针位置
                        curPtr++;
                    }else if(curTime > partialMatch.get(pos)){
                        // 超过Pos的时间就要早停，因为后续的都会不满足SEQ条件
                        break;
                    }else if(curTime >= partialMatch.get(pos - 1) && curTime <= partialMatch.get(pos)){
                        // 如果在这个中间，即满足了SEQ关系，WITHIN不需要检查

                        List<Long> timeList = new ArrayList<>(len + 1);
                        for(int j = 0; j < pos; ++j){
                            timeList.add(partialMatch.get(j));
                        }
                        timeList.add(curTime);
                        for(int j = pos; j < len; ++j){
                            timeList.add(partialMatch.get(j));
                        }
                        ans.add(timeList);
                    }
                }
            }
        }

        return ans;
    }


    /**
     * 新版的字符串类型的join，速度非常快，这是因为我们用了指针加速，还有用空间换时间的结果
     * @param pattern           要查询的事件模式
     * @param partialMatches    部分匹配元组
     * @param marks             已经匹配的变量编号
     * @param bucket            要被join的桶
     * @param k                 要被join的变量编号
     * @return                  新的部分匹配
     */
    public final List<PartialResultWithTime> joinWithRecord(EventPattern pattern, List<PartialResultWithTime> partialMatches,
                                                      List<Integer> marks, List<String> bucket, int k){
        // k所在的位置
        int pos = -1;
        int len = marks.size();
        for(int i = 0; i < len; ++i){
            if(k < marks.get(i)){
                pos = i;
            }
        }

        if(pos == -1){ pos = len;}

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // 加载属性类型数组和顺序变量数组已经时间戳对应的索引
        String[] attrTypes = schema.getAttrTypes();
        int timeIdx = schema.getTimestampIdx();
        // tau是查询模式的最大时间约束 单位是ms
        long tau = pattern.getTau();

        List<PartialResultWithTime> ans = new ArrayList<>();

        List<DependentConstraint> dcList = pattern.getDCListToJoin(marks, k);

        // [0, pos) k [pos, len)
        // 到时候会用这个加速，之所以能加速是因为事件时间有序
        int curPtr = 0;
        for(PartialResultWithTime partialMatch : partialMatches) {

            for(int i = curPtr; i < bucket.size(); ++i){
                String curRecord = bucket.get(i);
                String[] attrValues = curRecord.split(",");
                long curTime = Long.parseLong(attrValues[timeIdx]);

                if(pos == 0){
                    // SEQ检查 说明要被join的元素是第一个 如果这个时间戳大于部分匹配第一个时间戳 直接break
                    if(curTime > partialMatch.timeList().get(0)){
                        break;
                    }else if(partialMatch.timeList().get(0) - curTime > tau){
                        // 超过tau了， 更新指针位置，避免多次读取
                        curPtr++;
                    }else if(partialMatch.timeList().get(len-1) - curTime <= tau){
                        // 满足了SEQ关系，接下来检查是不是满足Within关系，满足的话那就检查依赖谓词约束
                        boolean satisfy = true;
                        if(dcList != null && dcList.size() != 0){
                            for(DependentConstraint dc : dcList) {
                                String varName1 = dc.getVarName1();
                                String varName2 = dc.getVarName2();
                                int varIndex1 = pattern.getVarNamePos(varName1);
                                int varIndex2 = pattern.getVarNamePos(varName2);
                                int cmpRecordPos = -1;
                                // 要比较的属性名字
                                String attrName = dc.getAttrName();
                                // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = (k == varIndex1);

                                if (isVarName1) {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex2) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                } else {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex1) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                }

                                String cmpRecord = partialMatch.matchList().get(cmpRecordPos);
                                String[] cmpAttrValues = cmpRecord.split(",");

                                long curValue, cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Integer.parseInt(attrValues[idx]);
                                    cmpValue = Integer.parseInt(cmpAttrValues[idx]);
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                                    curValue = (long) (Double.parseDouble(attrValues[idx]) * magnification);
                                    cmpValue = (long) (Double.parseDouble(cmpAttrValues[idx]) * magnification);
                                } else {
                                    throw new RuntimeException("Wrong index position.");
                                }
                                boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                if (!hold) {
                                    satisfy = false;
                                }
                            }
                        }
                        // 如果满足依赖谓词约束那就加进来
                        if(satisfy){
                            List<String> matchList = new ArrayList<>(len + 1);
                            matchList.add(curRecord);
                            matchList.addAll(partialMatch.matchList());
                            List<Long> timeList = new ArrayList<>(len + 1);
                            timeList.add(curTime);
                            timeList.addAll(partialMatch.timeList());
                            ans.add(new PartialResultWithTime(timeList, matchList));
                        }
                    }
                }else if(pos == len){
                    if(curTime < partialMatch.timeList().get(0)){
                        // 更新指针位置
                        curPtr++;
                    }else if(curTime - partialMatch.timeList().get(0) > tau){
                        break;
                    }else if(curTime >= partialMatch.timeList().get(len - 1)){
                        // 此时满足了Within和SEQ关系，开始进行依赖谓词约束检查
                        boolean satisfy = true;
                        if(dcList != null && dcList.size() != 0){
                            for(DependentConstraint dc : dcList) {
                                String varName1 = dc.getVarName1();
                                String varName2 = dc.getVarName2();
                                int varIndex1 = pattern.getVarNamePos(varName1);
                                int varIndex2 = pattern.getVarNamePos(varName2);
                                int cmpRecordPos = -1;
                                // 要比较的属性名字
                                String attrName = dc.getAttrName();
                                // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = (k == varIndex1);

                                if (isVarName1) {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex2) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                } else {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex1) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                }

                                String cmpRecord = partialMatch.matchList().get(cmpRecordPos);
                                String[] cmpAttrValues = cmpRecord.split(",");

                                long curValue, cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Integer.parseInt(attrValues[idx]);
                                    cmpValue = Integer.parseInt(cmpAttrValues[idx]);
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                                    curValue = (long) (Double.parseDouble(attrValues[idx]) * magnification);
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
                        // 如果满足依赖谓词约束那就加进来
                        if(satisfy){
                            List<String> matchList = new ArrayList<>(len + 1);
                            matchList.addAll(partialMatch.matchList());
                            matchList.add(curRecord);
                            List<Long> timeList = new ArrayList<>(len + 1);
                            timeList.addAll(partialMatch.timeList());
                            timeList.add(curTime);
                            ans.add(new PartialResultWithTime(timeList, matchList));
                        }
                    }
                }else{
                    // [0, pos) k [pos, len)
                    if(curTime < partialMatch.timeList().get(0)){
                        // 更新指针位置
                        curPtr++;
                    }else if(curTime > partialMatch.timeList().get(pos)){
                        // 超过Pos的时间就要早停，因为后续的都会不满足SEQ条件
                        break;
                    }else if(curTime >= partialMatch.timeList().get(pos - 1) && curTime <= partialMatch.timeList().get(pos)){
                        // 如果在这个中间，即满足了SEQ关系，WITHIN不需要检查
                        boolean satisfy = true;
                        if(dcList != null && dcList.size() != 0){
                            for(DependentConstraint dc : dcList) {
                                String varName1 = dc.getVarName1();
                                String varName2 = dc.getVarName2();
                                int varIndex1 = pattern.getVarNamePos(varName1);
                                int varIndex2 = pattern.getVarNamePos(varName2);
                                int cmpRecordPos = -1;
                                // 要比较的属性名字
                                String attrName = dc.getAttrName();
                                // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = (k == varIndex1);

                                if (isVarName1) {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex2) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                } else {
                                    for (int j = 0; j < marks.size(); j++) {
                                        if (marks.get(j) == varIndex1) {
                                            cmpRecordPos = j;
                                            break;
                                        }
                                    }
                                }

                                String cmpRecord = partialMatch.matchList().get(cmpRecordPos);
                                String[] cmpAttrValues = cmpRecord.split(",");

                                long curValue, cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Integer.parseInt(attrValues[idx]);
                                    cmpValue = Integer.parseInt(cmpAttrValues[idx]);
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                                    curValue = (long) (Double.parseDouble(attrValues[idx]) * magnification);
                                    cmpValue = (long) (Double.parseDouble(cmpAttrValues[idx]) * magnification);
                                } else {
                                    throw new RuntimeException("Wrong index position.");
                                }
                                boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                if (!hold) {
                                    satisfy = false;
                                }
                            }
                        }
                        // 如果满足依赖谓词约束那就加进来
                        if(satisfy){
                            List<String> matchList = new ArrayList<>(len + 1);

                            for(int j = 0; j < pos; ++j){
                                matchList.add(partialMatch.matchList().get(j));
                            }
                            matchList.add(curRecord);
                            for(int j = pos; j < len; ++j){
                                matchList.add(partialMatch.matchList().get(j));
                            }

                            List<Long> timeList = new ArrayList<>(len + 1);
                            for(int j = 0; j < pos; ++j){
                                timeList.add(partialMatch.timeList().get(j));
                            }
                            timeList.add(curTime);
                            for(int j = pos; j < len; ++j){
                                timeList.add(partialMatch.timeList().get(j));
                            }

                            ans.add(new PartialResultWithTime(timeList, matchList));
                        }
                    }
                }

            }
        }

        return ans;
    }

    /**
     * 传过来是byte[]类型的记录会好些 处理速度会更快
     * @param pattern           query pattern
     * @param partialMatches    partial matches
     * @param marks             已经完成join的变量编号
     * @param bucket            要被join的桶
     * @param k                 要被join的位置
     * @return                  返回部分匹配的结果
     */
    public final List<PartialBytesResultWithTime> joinWithBytesRecord(EventPattern pattern, List<PartialBytesResultWithTime> partialMatches,
                                                                List<Integer> marks, List<byte[]> bucket, int k){


        // k所在的位置
        int pos = -1;
        int len = marks.size();
        for(int i = 0; i < len; ++i){
            if(k < marks.get(i)){
                pos = i;
                break;//debug
            }
        }

        if(pos == -1){ pos = len;}

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // 加载属性类型数组和顺序变量数组已经时间戳对应的索引
        String[] attrTypes = schema.getAttrTypes();
        final int timeIdx = schema.getTimestampIdx();
        final long tau = pattern.getTau();

        List<PartialBytesResultWithTime> ans = new ArrayList<>(128);
        List<DependentConstraint> dcList = pattern.getDCListToJoin(marks, k);

        // [0, pos) k [pos, len)
        // 到时候会用这个加速，之所以能加速是因为事件时间有序
        int curPtr = 0;
        for(PartialBytesResultWithTime partialMatch : partialMatches) {
            long firstTime = partialMatch.timeList().get(0);
            long lastTime = partialMatch.timeList().get(len-1);

            for(int i = curPtr; i < bucket.size(); ++i){
                byte[] curRecord = bucket.get(i);
                long curTime = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));


                if(pos == 0){
                    // SEQ检查 说明要被join的元素是第一个 如果这个时间戳大于部分匹配第一个时间戳 直接break
                    if(curTime > partialMatch.timeList().get(0)){
                        break;
                    }else if(firstTime - curTime > tau){
                        // 超过tau了， 更新指针位置，避免多次读取
                        curPtr++;
                    }else if(lastTime - curTime <= tau){
                        // 如果出现事件完全相同情况，那么跳过
                        byte[] firstRecord = partialMatch.matchList().get(0);
                        boolean isEqual = Arrays.equals(curRecord, firstRecord);

                        if(!isEqual){
                            // 满足了SEQ关系，接下来检查是不是满足Within关系，满足的话那就检查依赖谓词约束
                            // 注意这样操作导致最后一个不是时间戳有序
                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;
                                    // 要比较的属性名字
                                    String attrName = dc.getAttrName();
                                    // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);

                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // 因为存储的时候已经放大了倍数了 因此这里不需要放大倍数了
                                        curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }
                                    boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                    if (!hold) {
                                        satisfy = false;
                                    }
                                }
                            }
                            // 如果满足依赖谓词约束那就加进来
                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);
                                matchList.add(curRecord);
                                matchList.addAll(partialMatch.matchList());
                                List<Long> timeList = new ArrayList<>(len + 1);
                                timeList.add(curTime);
                                timeList.addAll(partialMatch.timeList());
                                ans.add(new PartialBytesResultWithTime(timeList, matchList));
                            }
                        }
                    }
                }else if(pos == len){
                    if(curTime < firstTime){
                        // 更新指针位置
                        curPtr++;
                    }else if(curTime - firstTime > tau){
                        break;
                    }else if(curTime >= lastTime){
                        byte[] lastRecord = partialMatch.matchList().get(len - 1);
                        boolean isEqual = Arrays.equals(curRecord, lastRecord);

                        if(!isEqual){
                            // 此时满足了Within和SEQ关系，开始进行依赖谓词约束检查
                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;
                                    // 要比较的属性名字
                                    String attrName = dc.getAttrName();
                                    // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);

                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // 因为存储的时候已经放大了倍数了 因此这里不需要放大倍数了
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
                            // 如果满足依赖谓词约束那就加进来
                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);
                                matchList.addAll(partialMatch.matchList());
                                matchList.add(curRecord);
                                List<Long> timeList = new ArrayList<>(len + 1);
                                timeList.addAll(partialMatch.timeList());
                                timeList.add(curTime);
                                ans.add(new PartialBytesResultWithTime(timeList, matchList));
                            }
                        }
                    }
                }else{
                    // [0, pos) k [pos, len)
                    if(curTime < firstTime){
                        // 更新指针位置
                        curPtr++;
                    }else if(curTime > partialMatch.timeList().get(pos)){
                        // 超过Pos的时间就要早停，因为后续的都会不满足SEQ条件
                        break;
                    }else if(curTime >= partialMatch.timeList().get(pos - 1) && curTime <= partialMatch.timeList().get(pos)){

                        byte[] preRecord = partialMatch.matchList().get(pos - 1);
                        byte[] nextRecord = partialMatch.matchList().get(pos);

                        boolean isEqual1 = Arrays.equals(preRecord, curRecord);
                        boolean isEqual2 = Arrays.equals(nextRecord, curRecord);

                        if(!isEqual1 && !isEqual2){
                            // 如果在这个中间，即满足了SEQ关系，WITHIN不需要检查
                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;
                                    // 要比较的属性名字
                                    String attrName = dc.getAttrName();
                                    // 找到属性名字对应的索引 然后判断类型 最后传入dc中比较是否满足条件
                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);
                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // 因为存储的时候已经放大了倍数了 因此这里不需要放大倍数了
                                        curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }
                                    boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                    if (!hold) {
                                        satisfy = false;
                                    }
                                }
                            }
                            // 如果满足依赖谓词约束那就加进来
                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);

                                for(int j = 0; j < pos; ++j){
                                    matchList.add(partialMatch.matchList().get(j));
                                }
                                matchList.add(curRecord);
                                for(int j = pos; j < len; ++j){
                                    matchList.add(partialMatch.matchList().get(j));
                                }

                                List<Long> timeList = new ArrayList<>(len + 1);
                                for(int j = 0; j < pos; ++j){
                                    timeList.add(partialMatch.timeList().get(j));
                                }
                                timeList.add(curTime);
                                for(int j = pos; j < len; ++j){
                                    timeList.add(partialMatch.timeList().get(j));
                                }

                                ans.add(new PartialBytesResultWithTime(timeList, matchList));
                            }
                        }
                    }
                }
            }
        }

        return ans;
    }

}
