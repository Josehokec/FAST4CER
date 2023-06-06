package Common;

import java.util.HashMap;
import java.util.Map;

/*
this class has been discarded
 */
public class MetaInfo {
    private static HashMap<String, OldEventSchema> schemaMap;
    public static MetaInfo meta = new MetaInfo();

    public MetaInfo(){
        schemaMap = new HashMap<>();
    }
    public static MetaInfo getInstance(){
        return meta;
    }

    public OldEventSchema getEventSchema(String schemaName){
        return schemaMap.get(schemaName);
    }

    public boolean storeSchema(OldEventSchema s){
        String schemaName = s.getSchemaName();
        if(schemaMap.containsKey(schemaName)){
            return false;
        }else{
            schemaMap.put(schemaName, s);
            return true;
        }
    }

    public void printSchemaMap(){
        System.out.println("Schema map: ");
        for(Map.Entry entry : schemaMap.entrySet()){
            String mapKey = (String) entry.getKey();
            System.out.println("Key: \'" + mapKey+"\'");
        }
    }
}
