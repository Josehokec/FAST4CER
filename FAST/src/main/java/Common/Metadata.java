package Common;

import Method.Index;

import java.util.HashMap;
import java.util.Map;

/**
 * 存储元信息，单例模式
 * 根据schema名字得到EventSchema对象
 * 根据schema名字得到Index对象
 * 由于一个schema可能不止一个Index
 * 为了实现方便，这里假设一个schema就一个Index对象
 */
public class Metadata {
    private static HashMap<String, EventSchema> schemaMap;
    private static HashMap<String, Index> indexMap;
    public static Metadata meta = new Metadata();
    private Metadata(){
        schemaMap = new HashMap<>();
        indexMap = new HashMap<>();
    }

    public static Metadata getInstance(){
        return meta;
    }

    public EventSchema getEventSchema(String schemaName){
        return schemaMap.get(schemaName);
    }

    public boolean storeSchema(EventSchema s){
        String schemaName = s.getSchemaName();
        if(schemaMap.containsKey(schemaName)){
            return false;
        }else{
            schemaMap.put(schemaName, s);
            return true;
        }
    }

    public Index getIndex(String schemaName){
        return indexMap.get(schemaName);
    }

    public boolean bindIndex(String schemaName, Index index){
        if(indexMap.containsKey(schemaName)){
            return false;
        }else{
            indexMap.put(schemaName, index);
            return true;
        }
    }

    public void printSchemaMap(){
        System.out.println("Schema map: ");
        for(Map.Entry entry : schemaMap.entrySet()){
            String mapKey = (String) entry.getKey();
            System.out.println("Key: '" + mapKey+"'");
        }
    }
}
