package Common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
SEQ(IBM a; Oracle b; IBM c; Oracle d) AS p
FROM stock
WHERE 90 <= a.price <= 110 AND 90 <= b.price <= 110 AND c.price >= 125 AND d.price <= 75
WITHIN 10 minutes
RETURN COUNT(p) WITHIN 1 second
*/
public class OldEventPattern<T> {
    /*
    seqEventType.length = seqVariable.length = Theta.length
     */
    private String[] seqEventTypes;
    private String[] seqVariables;
    private String schemaName;
    private HashMap<String, List<PredicateConstraint<T>>> theta;
    private long tau;
    private QueryType queryType;

    private int maxQueryTime;

    public OldEventPattern(String input){
        String[] words = input.split("\n");

        String[] seqStatement = words[0].split("[\\(\\)]");

        String[] seqEvent = seqStatement[1].split("; ");
        seqEventTypes = new String[seqEvent.length];
        seqVariables = new String[seqEvent.length];
        for(int i = 0; i < seqEvent.length; ++i){
            String[] s = seqEvent[i].split(" ");
            seqEventTypes[i] = s[0];
            seqVariables[i] = s[1];
        }
        schemaName = words[1].split(" ")[1];

        String[] predicates = words[2].substring(6).split("AND");

        theta = new HashMap<>();
        for(int i = 0; i < predicates.length; i++){
            String curPredicate = predicates[i];
            String varName = null;
            String attrName = null;

            int cnt = 0;
            for(int idx = 0; idx < curPredicate.length(); idx++){
                if(curPredicate.charAt(idx) == '<'){
                    ++cnt;
                }
            }

            if(cnt == 2){
                int op1Pos = -1;
                int op2Pos = -1;
                int dotPos = -1;
                ComparedOperator op1 = null;
                ComparedOperator op2 = null;
                String value1 = null;
                String value2 = null;

                for(int k = 0; k < curPredicate.length(); ++k){
                    char ch = curPredicate.charAt(k);
                    if(ch == '<' && op1Pos == -1){
                        op1Pos = k;
                        value1 = curPredicate.substring(0, op1Pos).trim();
                        if(curPredicate.charAt(k + 1) == '='){
                            op1 = ComparedOperator.GE;
                        }else{
                            op1 = ComparedOperator.GT;
                        }
                    }else if(ch == '<' && op1Pos != -1){
                        op2Pos = k;
                        if(curPredicate.charAt(k + 1) == '='){
                            op2 = ComparedOperator.LE;
                            value2 = curPredicate.substring(k  + 2).trim();
                        }else{
                            op2 = ComparedOperator.LT;
                            value2 = curPredicate.substring(k  + 1).trim();
                        }
                    }else if(ch == '.'){
                        dotPos = k;
                    }
                }

                if(op1 == ComparedOperator.GE){
                    varName = curPredicate.substring(op1Pos + 2, dotPos).trim();
                }else{
                    varName = curPredicate.substring(op1Pos + 1, dotPos).trim();
                }
                attrName = curPredicate.substring(dotPos + 1, op2Pos).trim();

                MetaInfo meta = MetaInfo.getInstance();
                OldEventSchema s = meta.getEventSchema(schemaName);
                String attrType = s.getAttributeType(attrName);
                PredicateConstraint p1 = null;
                PredicateConstraint p2 = null;
                if(attrType.equals("INT")){
                    int convertValue1 = Integer.parseInt(value1);
                    int convertValue2 = Integer.parseInt(value2);
                    p1 = new PredicateConstraint<>(attrName, op1, convertValue1);
                    p2 = new PredicateConstraint<>(attrName, op2, convertValue2);
                }else if(attrType.equals("FLOAT")){
                    float convertValue1 = Float.parseFloat(value1);
                    float convertValue2 = Float.parseFloat(value2);
                    p1 = new PredicateConstraint<>(attrName,op1, convertValue1);
                    p2 = new PredicateConstraint<>(attrName,op2, convertValue2);
                }else{
                    System.out.println("attrType: \'" + attrType + "\'" + " is not defined!");
                }
                if(theta.containsKey(varName)){
                    List list = theta.get(varName);
                    list.add(p1);
                    list.add(p2);
                }else{
                    List<PredicateConstraint<T>> list = new ArrayList<>();
                    list.add(p1);
                    list.add(p2);
                    theta.put(varName,list);
                }
            }else{
                String value = null;
                ComparedOperator op = null;

                int dotPos = -1;
                int valBeginPos = -1;
                int opPos = -1;

                for(int idx = 0; idx < curPredicate.length(); ++idx){
                    if(curPredicate.charAt(idx) == '.'){
                        varName = curPredicate.substring(0, idx).trim();
                        dotPos = idx + 1;
                    }else if(curPredicate.charAt(idx) == '>'){
                        opPos = idx;
                        if(curPredicate.charAt(idx + 1) == '='){
                            op = ComparedOperator.GE;
                            valBeginPos = idx + 2;
                        }else {
                            op = ComparedOperator.GT;
                            valBeginPos = idx + 1;
                        }
                    }else if(curPredicate.charAt(idx) == '<'){
                        opPos = idx;
                        if(curPredicate.charAt(idx + 1) == '='){
                            op = ComparedOperator.LE;
                            valBeginPos = idx + 2;
                        }else{
                            op = ComparedOperator.LT;
                            valBeginPos = idx + 1;
                        }
                    }
                }
                attrName = curPredicate.substring(dotPos, opPos).trim();
                value = curPredicate.substring(valBeginPos).trim();

                MetaInfo meta = MetaInfo.getInstance();
                OldEventSchema s = meta.getEventSchema(schemaName);
                String attrType = s.getAttributeType(attrName);
                PredicateConstraint p = null;
                if(attrType.equals("INT")){
                    int convertValue = Integer.parseInt(value);
                    p = new PredicateConstraint<>(attrName, op, convertValue);
                }else if(attrType.equals("FLOAT")){
                    float convertValue = Float.parseFloat(value);
                    p = new PredicateConstraint<>(attrName, op, convertValue);
                }else{
                    System.out.println("attrType: \'" + attrType + "\'" + " is not defined!");
                }

                if(p != null){
                    if(theta.containsKey(varName)){
                        theta.get(varName).add(p);
                    }else{
                        List<PredicateConstraint<T>> list = new ArrayList<>();
                        list.add(p);
                        theta.put(varName,list);
                    }
                }
            }
        }


        String[] withinStatement = words[3].split(" ");
        assert withinStatement[0].equals("WITHIN"): "Pattern query statement encounter error";
        if(withinStatement[2].equals("hours") || withinStatement[2].equals("hour")){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 60 * 1000;
        }else if(withinStatement[2].indexOf("min") != -1){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 1000;
        }else if(withinStatement[2].indexOf("sec") != -1){
            tau = Integer.parseInt(withinStatement[1]) * 1000;
        }else{
            tau = Integer.parseInt(withinStatement[1]);
        }

        //处理RETURN语句
        String[] returnStatement = words[4].split(" ");
        assert returnStatement[0].equals("RETURN"): "Pattern query statement encounter error";
        if(returnStatement[1].indexOf("COUNT") != -1){
            queryType = QueryType.COUNT;
            float time = Float.parseFloat(returnStatement[3]);
            if(returnStatement[4].indexOf("sec") != -1){
                maxQueryTime = (int) time * 1000;
            }else if(returnStatement[4].indexOf("min") != -1){
                maxQueryTime = (int) time * 60 * 1000;
            }else if(returnStatement[4].indexOf("ms") != -1){
                maxQueryTime = (int) time;
            }else{
                maxQueryTime = (int) time;
            }
        }else if(returnStatement[1].indexOf("EXIST") != -1){
            queryType = QueryType.EXIST;
            maxQueryTime = -1;
        }else{
            System.out.println("This query type doesn't defined");
            queryType = QueryType.OTHERS;
        }
    }

    public String[] getSeqEventTypes() {
        return seqEventTypes;
    }

    public String[] getSeqVariables() {
        return seqVariables;
    }

    public long getTau(){
        return tau;
    }

    public String getSchemaName(){
        return schemaName;
    }
    public String getFirstEventType(){
        return seqEventTypes[0];
    }

    public List<PredicateConstraint<T>> getVarConstraints(String varName){
        return theta.get(varName);
    }



    public int getMaxQueryTime(){
        return  maxQueryTime;
    }



    public List<PredicateConstraint<T>> getIthEventConstraints(int ith){
        String varName = seqVariables[ith];
        return getVarConstraints(varName);
    }
    public int getPatternLen(){
        return seqEventTypes.length;
    }
    public boolean isCountQuery(){
        return queryType == QueryType.COUNT;
    }

    public boolean isExistQuery(){
        return queryType == QueryType.EXIST;
    }

    public void print(){
        System.out.println("query_schema: \'" + schemaName + "\'");
        for(int i = 0; i < seqEventTypes.length; i++){
            System.out.print("event_type:\'" +  seqEventTypes[i] +"\'"
                    + " variable_name: \'"+ seqVariables[i] + "\'" + " predicate_constraints:");
            List<PredicateConstraint<T>> list = getVarConstraints(seqVariables[i]);
            for(int j = 0; j < list.size(); ++j){
                PredicateConstraint p = list.get(j);
                System.out.print(" ");
                if(j == list.size() - 1){
                    p.println();
                }else{
                    p.print();
                }
            }


        }

        System.out.println("time_windows: " + tau + "ms");
        System.out.println("query_type: \'" + queryType + "\'" + " query_time: " + maxQueryTime + "ms");
    }

    public static void main(String[] args){
        String createStatement = "CREATE SCHEMA stock (ticker TYPE, open FLOAT, volume INT, time TIMESTAMP)";
        OldEventSchema s = new OldEventSchema(createStatement);
        MetaInfo meta = MetaInfo.getInstance();
        meta.storeSchema(s);


        String queryStatement = "SEQ(IBM a; Oracle b; IBM c; Oracle d) AS p\n" +
                                "FROM stock\n" +
                                "WHERE 90 <= a.open <= 110 AND 90 <= b.open <= 110 AND c.open >= 125 AND d.open <= 75\n" +
                                "WITHIN 10 mins\n" +
                                "RETURN COUNT(p) within 1 second";

        OldEventPattern p = new OldEventPattern(queryStatement);
        p.print();

    }
}
