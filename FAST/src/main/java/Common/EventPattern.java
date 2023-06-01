package Common;

import Condition.DependentConstraint;
import Condition.IndependentConstraint;

import java.util.*;

/**
 * 这里假设就是顺序的
 * 没有Conjunction('&') 和 Disjunction('|')算子
 */
public class EventPattern {
    private String[] seqEventTypes;                                     // 顺序事件类型
    private String[] seqVarNames;                                       // 顺序变量名字
    private String schemaName;                                          // schema名字
    private long tau;                                                   // 时间窗口
    private MatchStrategy strategy;                                     // 匹配策略
    private String returnStr;                                           // 返回语句
    private final Map<String, Integer> varMap;                          // 这个本来是多余的 但是为了加快后续某些操作这里加了这个变量
    private final HashMap<String, List<IndependentConstraint>> icMap;   //每个变量名字对应的独立谓词约束
    private List<DependentConstraint> dcList;                     //对应的依赖谓词约束 这里按照变量名字排序

    public EventPattern() {
        icMap = new HashMap<>();
        varMap = new HashMap<>();
        dcList = new ArrayList<>();
        strategy = MatchStrategy.SKIP_TILL_NEXT_MATCH;
    }

    public String[] getSeqEventTypes() {
        return seqEventTypes;
    }

    public void setSeqEventTypes(String[] seqEventTypes) {
        this.seqEventTypes = seqEventTypes;
    }

    public String[] getSeqVarNames() {
        return seqVarNames;
    }

    /**
     * 设置顺序变量的时候同时更新一下varMap
     * @param seqVarNames 顺序变量
     */
    public void setSeqVarNames(String[] seqVarNames) {
        this.seqVarNames = seqVarNames;
        for(int i = 0; i < seqVarNames.length; ++i){
            varMap.put(seqVarNames[i], i);
        }
    }


    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public HashMap<String, List<IndependentConstraint>> getIcMap() {
        return icMap;
    }

    /**
     * 根据变量名字得到对应的依赖谓词约束
     * @param varName 变量名字
     * @return 根据变量名字得到对应的icList
     */
    public List<IndependentConstraint> getICListUsingVarName(String varName){
        if(icMap.containsKey(varName)){
            return icMap.get(varName);
        }else{
            return new ArrayList<>();
        }
    }

    public List<DependentConstraint> getDcList() {
        return dcList;
    }

    /**
     * 是否含有依赖谓词
     * @return 含有的话是true，不含是false
     */
    public boolean existDC(){
        return dcList.size() > 0;
    }

    public long getTau() {
        return tau;
    }

    public void setTau(long tau) {
        this.tau = tau;
    }

    public MatchStrategy getStrategy(){
        return strategy;
    }

    public void setStrategy(String s){
        switch (s) {
            case "STRICT_CONTIGUOUS" -> strategy = MatchStrategy.STRICT_CONTIGUOUS;
            case "SKIP_TILL_NEXT_MATCH" -> strategy = MatchStrategy.SKIP_TILL_NEXT_MATCH;
            case "SKIP_TILL_ANY_MATCH" -> strategy = MatchStrategy.SKIP_TILL_ANY_MATCH;
            default -> System.out.println("Do not support match strategy '" + s + "'" +
                    ", we set default strategy is SKIP_TILL_NEXT_MATCH");
        }
    }

    public String getReturnStr() {
        return returnStr;
    }

    public void setReturnStr(String returnStr) {
        this.returnStr = returnStr;
    }

    /**
     * 得到依赖谓词所包含的全部属性名字
     * @return 属性名字
     */
    public List<String> getDCAllAttrNames(){
        if(dcList.size() == 0){
            return null;
        }else{
            Set<String> s = new HashSet<>();
            for(DependentConstraint dc : dcList){
                s.add(dc.getAttrName());
            }
            return new ArrayList<>(s);
        }
    }

    /**
     * 根据变量名字的位置进行排序
     * 假设Pattern是(a, b, c)
     * DC的格式是a.open <= b.open
     * 我们根据变量名字的顺序排序 -> 最大的变量位置按照降序排序
     */
    public void sortDC(){
        Comparator<DependentConstraint> cmp = (o1, o2) -> {
            int max1 = maxVarIdx(o1.getVarName1(), o1.getVarName2());
            int max2 = maxVarIdx(o2.getVarName1(), o2.getVarName2());
            return max1 - max2;
        };
        dcList.sort(cmp);
    }

    /**
     * 根据模式变量名字返回模式的位置<br>
     * 假设Pattern是(A a, B b, C c)<br>
     * 则getVarNamePos(a) = 0; getVarNamePos(b) = 1; getVarNamePos(c) = 2;
     * @param varName 模式变量名字
     * @return 所在的位置
     */
    public int getVarNamePos(String varName){
        Integer pos = varMap.get(varName);
        if(pos == null){
            throw new IllegalStateException("Variable name has error.");
        }
        return pos;
    }

    /**
     * 得到DC的变量名字的最大索引
     * 假设查询的模式是(A a, B b, C c)
     * DC 是 a.attr < b.attr
     * idx(a) = 0, idx(b) = 1, idx(c) = 2
     * getDCMaxNum(dc) = 1
     * @param dc 依赖约束
     * @return 最大的索引
     */
    public int getDCMaxNum(DependentConstraint dc){
        return maxVarIdx(dc.getVarName1(), dc.getVarName2());
    }

    /**
     * 得到包含第i个变量的依赖谓词列表，假设DC已经排序好了
     * @param ith i-th变量名字
     * @return 依赖谓词列表
     */
    public List<DependentConstraint> getContainIthVarDCList(int ith){
        List<DependentConstraint> ans = new ArrayList<>();
        // 这种方式效率复杂度是O(N)可以改进
        for(DependentConstraint dc : dcList){
            if(getDCMaxNum(dc) == ith){
                ans.add(dc);
            }else if(getDCMaxNum(dc) > ith){
                // 由于dcList已经排序，因此可以break出来
                break;
            }
        }
        return ans;
    }

    public int maxVarIdx(String varName1, String varName2){
        Integer idx1 = varMap.get(varName1);
        Integer idx2 = varMap.get(varName2);
        if(idx2 == null || idx1 == null){
            throw new IllegalStateException("Variable name has error.");
        }
        return idx1 > idx2 ? idx1 : idx2;
    }

    /**
     * 这个函数代价太大了 舍弃
     * @return 独立约束map
     */
    public HashMap<String, List<DependentConstraint>> getDCListHashMap(){
        if(dcList == null){
            return null;
        }else{
            HashMap<String, List<DependentConstraint>> ans = new HashMap<>();

            for(DependentConstraint dc : dcList){
                String varName1 = dc.getVarName1();
                String varName2 = dc.getVarName2();

                if(ans.containsKey(varName1)){
                    ans.get(varName1).add(dc);
                }else{
                    List<DependentConstraint> list = new ArrayList<>();
                    list.add(dc);
                    ans.put(varName1, list);
                }

                if(ans.containsKey(varName2)){
                    ans.get(varName2).add(dc);
                }else{
                    List<DependentConstraint> list = new ArrayList<>();
                    list.add(dc);
                    ans.put(varName2, list);
                }
            }
            return ans;
        }
    }

    /**
     * 得到join需要使用的依赖谓词
     * @param hasJoinPos 已经join的列表
     * @param waitJoinPos 等待join的位置
     * @return 依赖谓词列表
     */
    public List<DependentConstraint> getDCListToJoin(List<Integer> hasJoinPos, int waitJoinPos){
        List<DependentConstraint> ans = new ArrayList<>();
        for(DependentConstraint dc : dcList){
            String varName1 = dc.getVarName1();
            String varName2 = dc.getVarName2();

            int varIndex1 = varMap.get(varName1);
            int varIndex2 = varMap.get(varName2);

            if(waitJoinPos == varIndex1){
                if(hasJoinPos.contains(varIndex2)){
                    ans.add(dc);
                }
            }else if(waitJoinPos == varIndex2){
                if(hasJoinPos.contains(varIndex1)){
                    ans.add(dc);
                }
            }
        }
        return ans;
    }

    public void print(){
        System.out.println("----------------------Event Pattern Information----------------------");
        System.out.println("query schema: '" + schemaName + "'");
        System.out.println("match strategy: '" + strategy + "'");
        for(int i = 0; i < seqEventTypes.length; i++){
            System.out.println("event type:'" +  seqEventTypes[i] + "'"
                    + " variable_name: '"+ seqVarNames[i] + "'" + " independent_predicate_constraints:");

            List<IndependentConstraint> icList = getICListUsingVarName(seqVarNames[i]);
            if(icList == null){
                System.out.println("null");
            }else{
                for(IndependentConstraint ic : icList){
                    ic.print();
                }
            }

        }

        System.out.println("dependent_predicate_constraints:");
        if(dcList.size() == 0){
            System.out.println("null");
        }else{
            for(DependentConstraint dc : dcList){
                dc.print();
            }
        }

        System.out.println("query time windows: " + tau + "ms");
        System.out.println("return statement: " + returnStr);
        System.out.println("-----------------------------------------------------------------------");
    }
}
