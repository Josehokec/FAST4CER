package Common;

import Condition.DependentConstraint;
import Condition.IndependentConstraint;
import Method.*;
import Store.EventStore;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 一个简单版本的查询语言解释器
 * 语句目前支持这四种：
 * 1. CREATE TABLE
 * 2. CREATE INDEX
 * 3. ALTER
 * 4. PATTERN
 */
public class StatementParser {

    /**
     * 把语句变成大写，并且删掉前后空格，把连续两个空格变成一个空格
     * @param statement 输入的语句
     * @return 返回转换大写后的字符串
     */
    public static String convert(String statement){
        return statement.toUpperCase().trim().replaceAll("\\s+", " ");
    }

    /**
     * 根据创建表语句new Schema和Store
     * CREATE TABLE stock (ticker TYPE, open FLOAT.2, volume INT, time TIMESTAMP)
     * 假设只支持float+int+double类型的数据
     * 当类型是float和double时候需要指定保留小数点后几位
     * @param statement 创建表的语句
     */
    public static void createTable(String statement){
        EventSchema schema = new EventSchema();

        // 根据括号把字符串分成两个部分
        String[] parts = statement.split("[()]");
        String schemaName = parts[0].split(" ")[2].trim();
        schema.setSchemaName(schemaName);

        // attributes: ['ticker TYPE', ' open FLOAT.2', ' volume INT', ' time TIMESTAMP']
        String[] attributes = parts[1].split(",");

        String[] attrNames = new String[attributes.length];
        String[] attrTypes = new String[attributes.length];
        long[] attrMinValues = new long[attributes.length];
        long[] attrMaxValues = new long[attributes.length];
        int[] decimalLens = new int[attributes.length];

        // 注意RangeBitmap目前只能支持插入大于等于0的整数
        // 创建索引之前一定会规定值范围
        // 只能在INT、FLOAT和DOUBLE上创建索引
        //String[] supportValueType = {"TYPE", "INT", "FLOAT", "DOUBLE", "TIMESTAMP"};

        for(int i = 0; i < attributes.length; ++i){
            String[] splits = attributes[i].trim().split(" ");
            attrNames[i] = splits[0].trim();
            attrTypes[i] = splits[1].trim();

            if(attrTypes[i].contains("FLOAT") || attrTypes[i].contains("DOUBLE")){
                int dotPos = -1;
                for(int j = 0; j < attrTypes[i].length(); ++j){
                    if(attrTypes[i].charAt(j) == '.'){
                        dotPos = j;
                    }
                }
                decimalLens[i] = Integer.parseInt(attrTypes[i].substring(dotPos + 1));
            }else{
                boolean flag = attrTypes[i].equals("TYPE") ||  attrTypes[i].equals("INT") || attrTypes[i].equals("TIMESTAMP");
                assert flag : "Class StatementParser - Do not support '" + attrTypes[i] + "' value type";
            }

            schema.insertAttrName(attrNames[i], i);
        }

        schema.setAttrNames(attrNames);
        // 这里会计算出recordSize
        schema.setAttrTypes(attrTypes);
        schema.setAttrMaxValues(attrMaxValues);
        schema.setAttrMinValues(attrMinValues);
        schema.setDecimalLens(decimalLens);

        int recordSize = schema.getStoreRecordSize();
        EventStore store = new EventStore(schemaName, recordSize);
        schema.setStore(store);

        Metadata metadata = Metadata.getInstance();
        if(metadata.storeSchema(schema)){
            System.out.println("Create schema successfully.");
        }else{
            System.out.println("Create schema fail, this schema name '"
                    + schema.getSchemaName() + "' has existing.");
        }
    }

    /**
     * 没有实现插入约束性检查
     * ALTER TABLE STOCK ADD CONSTRAINT OPEN IN RANGE [0,1000]
     * @param statement 设置属性范围的语句
     */
    public static void setAttrValueRange(String statement){
        String[] splits = statement.split(" ");
        String schemaName = splits[2];
        String attrName = splits[5];
        int len = splits[8].length();
        String range = splits[8].substring(1, len - 1);

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);

        if(schema == null){
            throw new RuntimeException("No existing schema '" + schemaName + "'");
        }else{
            int idx = schema.getAttrNameIdx(attrName);
            String type = schema.getIthAttrType(idx);
            String min = range.split(",")[0];
            String max = range.split(",")[1];
            if(type.equals("INT") || type.equals("TYPE")){
                schema.setIthAttrMinValue(idx, Integer.parseInt(min));
                schema.setIthAttrMaxValue(idx, Integer.parseInt(max));
            }else if(type.contains("FLOAT")){
                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                long minValue = (long) (Float.parseFloat(min) * magnification);
                long maxValue = (long) (Float.parseFloat(max) * magnification);
                schema.setIthAttrMinValue(idx, minValue);
                schema.setIthAttrMaxValue(idx, maxValue);
            }else if(type.contains("DOUBLE")){
                int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
                long minValue = (long) (Double.parseDouble(min) * magnification);
                long maxValue = (long) (Double.parseDouble(max) * magnification);
                schema.setIthAttrMinValue(idx, minValue);
                schema.setIthAttrMaxValue(idx, maxValue);
            }
        }
    }

    /**
     * CREATE INDEX index_name1 USING FAST ON stock(open, volume)
     * @param statement 创建索引语句
     * @return 返回要创建的索引
     */
    public static Index createIndex(String statement){
        String[] parts = statement.split("[()]");

        String[] splits = parts[0].split(" ");
        String indexName = splits[2];
        String indexType = splits[4];
        String schemaName = splits[6].trim();
        // 注意这里可能有空格
        String[] indexAttrNames = parts[1].split(",");

        Index index;
        switch (indexType) {
            case "FAST_V2" -> index = new DiscardFast(indexName);
            case "FAST" -> index = new OptimalFastIndex(indexName);
            case "B_PLUS_TREE" -> index = new NaiveIndexUsingBPlusTree(indexName);
            case "B_PLUS_TREE_PLUS" -> index = new NaiveBPlusTreePlus(indexName);
            case "SKIP_LIST" -> index = new NaiveIndexUsingSkipList(indexName);
            case "SKIP_LIST_PLUS" -> index = new NaiveSkipListPlus(indexName);
            case "R_TREE" -> index = new NaiveIndexUsingRTree(indexName);
            case "R_TREE_PLUS" -> index = new NaiveRTreePlus(indexName);
            case "STATE_OF_THE_ART", "INTERVAL_SCAN" -> index = new IntervalScan(indexName);
            case "TWO_RTREE_PLUS" -> index = new NaiveTwoRTreesPLus(indexName);
            default -> {
                System.out.println("Can not support " + indexType + " index.");
                index = new OptimalFastIndex(indexName);
            }
        }


        for(String name : indexAttrNames){
            index.addIndexAttrNameMap(name.trim());
        }
        // 绑定之前创建的schema
        Metadata metadata = Metadata.getInstance();
        index.setSchema(metadata.getEventSchema(schemaName));
        if(metadata.bindIndex(schemaName, index)){
            System.out.println("Create '" + indexType + "' index successfully.");
        }else{
            System.out.println("Class StatementParser - Index has been created before.");
        }

        return index;
    }

    /**
     * PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
     * FROM stock
     * USING SKIP_TILL_NEXT_MATCH
     * WHERE 90 <= a.open <= 110 AND 90 <= b.open <= 110 AND c.open >= 125 AND d.open <= 75
     * WITHIN 10 min
     * RETURN COUNT(*)
     * @param statement     query statement (String)
     * @return              query pattern   (Object)
     */
    public static EventPattern queryPattern(String statement){
        EventPattern p = new EventPattern();
        // 一共有6行，每行每行处理
        String[] sentences = statement.split("\n");
        // 第一行是顺序模式 先转成大写
        readFirstSentence(p, sentences[0].toUpperCase());
        // 第2行指定第是表名字
        String schemaName = sentences[1].toUpperCase().split(" ")[1];
        p.setSchemaName(schemaName);
        // 第3行是匹配策略
        String matchStrategy = sentences[2].toUpperCase().split(" ")[1];
        p.setStrategy(matchStrategy);
        // 第4行是谓词约束，比较难处理
        readFourthSentence(p, sentences[3].toUpperCase(), schemaName);
        // 第5行是WITHIN语句(WITHIN 10 min)，默认的事件单位是毫秒
        long tau;
        String[] withinStatement = sentences[4].toUpperCase().split(" ");
        if(withinStatement[2].contains("HOUR")){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 60 * 1000L;
        }else if(withinStatement[2].contains("MIN")){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 1000L;
        }else if(withinStatement[2].contains("SEC")){
            tau = Integer.parseInt(withinStatement[1]) * 1000L;
        }else{
            tau = Integer.parseInt(withinStatement[1]);
        }
        p.setTau(tau);
        //第6行是RETURN语句 RETURN COUNT(*)
        String returnStr = sentences[5].toUpperCase().substring(6);
        p.setReturnStr(returnStr);

        return p;
    }

    private static void readFirstSentence(EventPattern p, String firstSentence){
        // 第一行是SEQ语句 PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
        String[] seqStatement = firstSentence.split("[()]");
        // seqEvent = "IBM a, Oracle b, IBM c, Oracle d"

        String[] seqEvent = seqStatement[1].split(",");
        String[] seqEventTypes = new String[seqEvent.length];
        String[] seqVarNames = new String[seqEvent.length];

        for(int i = 0; i < seqEvent.length; ++i){
            String[] s = seqEvent[i].trim().split(" ");
            seqEventTypes[i] = s[0];
            seqVarNames[i] = s[1].trim();
        }

        p.setSeqEventTypes(seqEventTypes);
        p.setSeqVarNames(seqVarNames);
    }

    /**
     * 第3行是where语句<br>
     * 谓词约束支持三种格式的regex1和regex2和regex3<br>
     * 把变量名字、属性名字、操作算子和值读取到，然后以变量名字作为key去存储这个变量的相关谓词约束
     * @param p 事件模式
     * @param thirdSentence 事件模式查询语句第三行
     * @param schemaName 事件schema
     */
    private static void readFourthSentence(EventPattern p, String thirdSentence, String schemaName){
        // 90 <= a.open <= 110 AND 90.1 <= b.open <= 110.1 AND c.open >= 125 AND d.open <= 75
        String[] predicates = thirdSentence.substring(6).split("AND");
        String regex1 = "^([-+])?\\d+(\\.\\d+)? *<=? *[A-Za-z0-9]+.[A-Za-z0-9]+ *<=? *([-+])?\\d+(\\.\\d+)?";
        String regex2 = "^[A-Za-z0-9]+.[A-Za-z0-9]+ *[><]=? *([-+])?\\d+(\\.\\d+)?";
        String regex3 = "^[A-Za-z0-9]+.[A-Za-z0-9]+ *([*/] *([-+])?\\d+(\\.\\d+)?)? *([+-] *([-+])?\\d+(\\.\\d+)?)? *[><=]=? *[A-Za-z0-9]+.[A-Za-z0-9]+ *([*/] *([-+])?\\d+(\\.\\d+)?)? *([+-] *([-+])?\\d+(\\.\\d+)?)?";

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);

        for (String predicate : predicates) {
            // 把前后空格去掉
            String curPredicate = predicate.trim();
            if(Pattern.matches(regex1, curPredicate)){
                // number <=? varName.attrName <=? number
                parseIndependentConstraint1(p, curPredicate, schema);
            }else if(Pattern.matches(regex2, curPredicate)){
                // varName.attrName [><]=? number
                parseIndependentConstraint2(p, curPredicate, schema);
            }else if(Pattern.matches(regex3, curPredicate)){
                // varName.attrName * number + number  [><=]=? varName.attrName * number + number
                parseDependentConstraint(p, curPredicate, schema);
            }else{
                System.out.println("current predicate: '" + curPredicate + "'" + " is illegal.");
                throw new RuntimeException("query pattern exists error.");
            }
        }
    }

    /**
     * 处理number <=? varName.attrName <=? number这种格式的谓词
     * @param pattern           查询的模式
     * @param curPredicate      当前的谓词
     * @param schema            schema
     */
    private static void parseIndependentConstraint1(EventPattern pattern, String curPredicate, EventSchema schema){
        int aoPos1 = -1;
        int aoPos2 = -1;
        ComparedOperator cmp1, cmp2;
        for(int k = 0; k < curPredicate.length(); ++k) {
            char ch = curPredicate.charAt(k);
            if(ch == '<'){
                if(aoPos1 == -1){
                    aoPos1 = k;
                }else{
                    aoPos2 = k;
                    break;
                }
            }
        }
        int midPos;
        String value1 = curPredicate.substring(0, aoPos1).trim();
        if(curPredicate.charAt(aoPos1 + 1) == '='){
            cmp1 = ComparedOperator.GE;
            midPos = aoPos1 + 2;
        }else{
            cmp1 = ComparedOperator.GT;
            midPos = aoPos1 + 1;
        }

        String varNameAndAttrName  = curPredicate.substring(midPos, aoPos2).trim();
        String[] split = varNameAndAttrName.split("\\.");
        String varName = split[0];
        String attrName = split[1];


        String value2;
        if(curPredicate.charAt(aoPos2 + 1) == '='){
            cmp2 = ComparedOperator.LE;
            value2 = curPredicate.substring(aoPos2 + 2).trim();
        }else{
            cmp2 = ComparedOperator.LT;
            value2 = curPredicate.substring(aoPos2 + 1).trim();
        }

        IndependentConstraint ic = new IndependentConstraint(attrName, cmp1, value1, cmp2, value2, schema);

        if(pattern.getIcMap().containsKey(varName)){
            List<IndependentConstraint> icList = pattern.getIcMap().get(varName);
            icList.add(ic);
        }else{
            List<IndependentConstraint> icList = new ArrayList<>(4);
            icList.add(ic);
            pattern.getIcMap().put(varName,icList);
        }
    }

    /**
     * 处理varName.attrName [><]=? number这种格式的谓词
     * @param pattern           查询的模式
     * @param curPredicate      当前的谓词
     * @param schema            schema
     */
    private static void parseIndependentConstraint2(EventPattern pattern, String curPredicate, EventSchema schema){
        // 找到小于号或者大于号的位置
        int aoPos = -1;
        ComparedOperator cmp;
        for(int k = 0; k < curPredicate.length(); ++k) {
            char ch = curPredicate.charAt(k);
            if(ch == '<' || ch == '>'){
                aoPos = k;
                break;
            }
        }

        String left = curPredicate.substring(0, aoPos).trim();
        String[] split = left.split("\\.");
        String varName = split[0];
        String attrName = split[1];
        String value;

        if(curPredicate.charAt(aoPos + 1) == '='){
            value = curPredicate.substring(aoPos + 2).trim();
            if(curPredicate.charAt(aoPos) == '>'){
                cmp = ComparedOperator.GE;
            }else{
                cmp = ComparedOperator.LE;
            }
        }
        else{
            value = curPredicate.substring(aoPos + 1).trim();
            if(curPredicate.charAt(aoPos) == '>'){
                cmp = ComparedOperator.GT;
            }else{
                cmp = ComparedOperator.LT;
            }
        }

        IndependentConstraint ic = new IndependentConstraint(attrName, cmp, value, schema);
        if(pattern.getIcMap().containsKey(varName)){
            List<IndependentConstraint> icList = pattern.getIcMap().get(varName);
            icList.add(ic);
        }else{
            List<IndependentConstraint> icList = new ArrayList<>(4);
            icList.add(ic);
            pattern.getIcMap().put(varName,icList);
        }
    }

    /**
     * 目前只处理两种形式的依赖谓词<br>
     * 注意：由于不支持浮点数，const1和const2会自动转换<br>
     * case 1: a.open <= b.open<br>
     * case 2: a.open * 3 - 5 >= b.open / 2 + 3<br>
     * @param pattern           查询的事件模式
     * @param curPredicate      当前谓词
     * @param schema            模式
     */
    public static void parseDependentConstraint(EventPattern pattern, String curPredicate, EventSchema schema){

        // 如果flag是true说明是case 2, 如果flag是false说明是case 1
        boolean flag = curPredicate.contains("+") || curPredicate.contains("-") ||
                curPredicate.contains("*") || curPredicate.contains("/");

        if(flag){
            DependentConstraint dc = new DependentConstraint();
            for(int i = 0; i < curPredicate.length(); ++i){
                if(curPredicate.charAt(i) == '<' && curPredicate.charAt(i + 1) == '='){
                    dc.serCmp(ComparedOperator.LE);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 2), schema);
                    break;
                }else if(curPredicate.charAt(i) == '<'){
                    dc.serCmp(ComparedOperator.LT);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 1), schema);
                    break;
                }else if(curPredicate.charAt(i) == '>' && curPredicate.charAt(i + 1) == '='){
                    dc.serCmp(ComparedOperator.GE);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 2), schema);
                    break;
                }else if(curPredicate.charAt(i) == '>'){
                    dc.serCmp(ComparedOperator.GT);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 1), schema);
                    break;
                }
            }
            pattern.getDcList().add(dc);
        }
        else{
            // format: a.open <= b.open
            // 记录第一个点和第二个点的位置
            int firstDot = -1;
            int secondDot = -1;
            //记录比较符号的位置
            int opPos = -1;
            //记录第一个变量名字和第二个变量名字
            String firstVarName =null;
            String secondVarName = null;
            ComparedOperator op = null;
            for(int i = 0; i < curPredicate.length(); ++i){
                char ch = curPredicate.charAt(i);
                // 第一个点的位置
                if(ch == '.' && firstDot == -1){
                    firstDot = i;
                    // [0,i)
                    firstVarName = curPredicate.substring(0,firstDot).trim();
                }else if(ch == '>' || ch == '<' || ch == '='){
                    opPos = i;
                    if(ch == '='){
                        op = ComparedOperator.EQ;
                    }else if(curPredicate.charAt(i + 1) == '='){
                        op = ch == '<' ? ComparedOperator.LE : ComparedOperator.GE;
                        // skip a position
                        i++;
                    }else{
                        op = ch == '<' ? ComparedOperator.LT : ComparedOperator.GT;
                    }
                }else if(ch == '.'){
                    secondDot = i;
                    // [opPos + 1, secondDot)
                    if(op == ComparedOperator.GE || op == ComparedOperator.LE){
                        secondVarName = curPredicate.substring(opPos + 2, secondDot).trim();
                    }else{
                        secondVarName = curPredicate.substring(opPos + 1, secondDot).trim();
                    }
                }
            }

            String attrName1 = curPredicate.substring(firstDot + 1, opPos).trim();
            String attrName2 = curPredicate.substring(secondDot + 1).trim();

            if(attrName1.equals(attrName2)){
                DependentConstraint dc = new DependentConstraint(attrName1, firstVarName, secondVarName, op);
                pattern.getDcList().add(dc);
            }else{
                // 两个属性名字不一样 出现错误了
                throw new RuntimeException("Dependent Constraint has error.");
            }
        }
    }
}
