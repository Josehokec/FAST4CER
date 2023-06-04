package Method;

import Common.EventPattern;
import Common.EventSchema;
import Common.ReservoirSampling;
import JoinStrategy.AbstractJoinStrategy;
import JoinStrategy.Tuple;

import java.util.HashMap;
import java.util.List;

/**
 *
 * indexNUm存储了有多少列要创建索引
 * indexName 索引的名字
 * schema 该索引对应的schema
 * reservoir 保存采样的数据
 * indexAttrNameMap存储了要创建的索引的名字和对应的编号
 *
 */
public abstract class Index {
    public int indexAttrNum;                            // 索引的属性数量
    public int autoIndices;                             // 记录索引
    private String indexName;                           // 索引的名字
    public EventSchema schema;                          // 事件模式
    public HashMap<String, Double> arrivals;            // 每个事件类型的到达率
    public ReservoirSampling reservoir;                 // 蓄水池采样
    public HashMap<String, Integer> indexAttrNameMap;   // 索引属性名字列表

    Index(String indexName){
        this.indexName = indexName;
        indexAttrNum = 0;
        autoIndices = 0;
        indexAttrNameMap = new HashMap<>();
    }

    public String getIndexName() {
        return indexName;
    }

    public EventSchema getSchema() {
        return schema;
    }

    public void setSchema(EventSchema schema) {
        this.schema = schema;
    }

    public void addIndexAttrNameMap(String attrName){
        indexAttrNameMap.put(attrName, indexAttrNum++);
    }

    public int getIndexNameId(String attrName){
        return indexAttrNameMap.get(attrName);
    }

    /**
     * 得到索引属性的名字，按照编号排序
     * @return 索引名字数组
     */
    public String[] getIndexAttrNames(){
        String[] indexNames = new String[indexAttrNum];
        indexAttrNameMap.forEach((k, v) -> indexNames[v] = k);
        return indexNames;
    }

    public abstract void initial();
    public abstract boolean insertRecord(String record);

    public abstract boolean insertBatchRecord(String[] record);

    public abstract int countQuery(EventPattern pattern, AbstractJoinStrategy join);
    public abstract List<Tuple> tupleQuery(EventPattern pattern, AbstractJoinStrategy join);

    public abstract void print();
}
