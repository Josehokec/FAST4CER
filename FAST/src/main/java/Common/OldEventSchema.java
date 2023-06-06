package Common;

import java.util.HashMap;
import java.util.Map;

/**
 * this class has been discarded
 */

public class OldEventSchema {
    private String schemaName;
    private String[] attributeNames;
    private String[] attributeTypes;
    private double[] attributeMinValues;
    private double[] attributeMaxValues;
    private static double eventArrivalRate;
    private int eventTypeID;

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setAttributeNames(String[] attributeNames) {
        this.attributeNames = attributeNames;
    }

    public void setAttributeTypes(String[] attributeTypes) {
        this.attributeTypes = attributeTypes;
    }

    public double[] getAttributeMinValues() {
        return attributeMinValues;
    }

    public void setAttributeMinValues(double[] attributeMinValues) {
        this.attributeMinValues = attributeMinValues;
    }

    public double[] getAttributeMaxValues() {
        return attributeMaxValues;
    }

    public void setAttributeMaxValues(double[] attributeMaxValues) {
        this.attributeMaxValues = attributeMaxValues;
    }

    public int getEventTypeID() {
        return eventTypeID;
    }

    public void setEventTypeID(int eventTypeID) {
        this.eventTypeID = eventTypeID;
    }

    public int getEventTypeBitLen() {
        return eventTypeBitLen;
    }

    public void setEventTypeBitLen(int eventTypeBitLen) {
        this.eventTypeBitLen = eventTypeBitLen;
    }

    public static HashMap<String, Integer> getEventTypeMap() {
        return eventTypeMap;
    }

    public static void setEventTypeMap(HashMap<String, Integer> eventTypeMap) {
        OldEventSchema.eventTypeMap = eventTypeMap;
    }

    public void setIndexMap(HashMap<String, Integer> indexMap) {
        this.indexMap = indexMap;
    }

    private int eventTypeBitLen;
    private static HashMap<String, Integer> eventTypeMap;
    private HashMap<String, Integer> indexMap;


    public OldEventSchema(){

    }

    public OldEventSchema(String schemaString){

        schemaString = schemaString.replaceAll("\\s+", " ");
        //split schema name and attribute list
        String[] words = schemaString.split("[\\(\\)]");
        schemaName = words[0].split(" ")[2].trim();
        String[] attributes = words[1].split(",");
        attributeNames = new String[attributes.length];
        attributeTypes = new String[attributes.length];
        attributeMinValues = new double[attributes.length];
        attributeMaxValues = new double[attributes.length];

        eventArrivalRate = 0;
        eventTypeID = 1;
        eventTypeBitLen = 0;
        eventTypeMap = new HashMap<>();
        indexMap = new HashMap<>();

        boolean existType = false;
        boolean existTimestamp = false;
        int index = 1;
        for(int i = 0; i < attributes.length; ++i) {
            String attribute = attributes[i];
            String[] split = attribute.strip().split(" ");
            if(split[1].equals("TYPE")){
                existType = true;
                attributeNames[0] = split[0];
                attributeTypes[0] = split[1];
                attributeMaxValues[0] = Integer.MAX_VALUE;
                attributeMinValues[0] = 1;
                //this.markIndex[0] = false;
            }else if(split[1].equals("TIMESTAMP")){
                existTimestamp = true;
                attributeNames[attributes.length - 1] = split[0];
                attributeTypes[attributes.length - 1] = split[1];
                attributeMaxValues[attributes.length - 1] = Long.MAX_VALUE;
                attributeMinValues[attributes.length - 1] = 0;
                //markIndex[attributes.length - 1] = false;
            }else{
                this.attributeNames[index] = split[0];
                this.attributeTypes[index] = split[1];
                //markIndex[index] = false;
                // 设置值范围
                if(split[1].equals("INT")){
                    attributeMinValues[index] = Integer.MIN_VALUE;
                    attributeMaxValues[index] = Integer.MAX_VALUE;
                }else if(split[1].equals("FLOAT")){
                    attributeMinValues[index] = Float.MIN_VALUE;
                    attributeMaxValues[index] = Float.MAX_VALUE;
                }
                ++index;
            }
        }

        if(existTimestamp && existType){
            System.out.println("Create schema successfully.");
            print();
        }else{
            System.out.println("Create fail, the created schema lack type or timestamp attribute.");
        }

    }

    public String getSchemaName() {
        return schemaName;
    }

    public double getEventArrivalRate() {
        return eventArrivalRate;
    }

    public void setEventArrivalRate(double eventArrivalRate) {
        this.eventArrivalRate = eventArrivalRate;
    }
    public String[] getAttributeNames() {
        return attributeNames;
    }

    public boolean isIndex(String attrName){
        return indexMap.containsKey(attrName);
    }

    public HashMap<String, Integer> getIndexMap() {
        return indexMap;
    }

    public int getEventTypeCode(String eventType){
        if(eventTypeMap.containsKey(eventType)){
            return eventTypeMap.get(eventType);
        }else{
            eventTypeMap.put(eventType, eventTypeID);
            return eventTypeID++;
        }
    }

    public boolean setIndex(String attrName){
        // 首先要判断attrName是不是在attNames中
        for(int i = 0; i < attributeNames.length; ++i){
            if(attrName.equals(attributeNames[i])){
                indexMap.put(attrName, i);
                return true;
            }
        }
        System.out.println("Class EVentSchema - This schema do not have attribute \'" + attrName + "\'");
        return false;
    }

    /* input: ticker,open,volume */
    public void setAttributeIndex(String indexVariables){
        String[] attrNames= indexVariables.split(",");
        for(String attrName : attrNames){
            setIndex(attrName);
        }
    }

    public int getSumIndexNum(){
        return indexMap.size();
    }
    public String[] getAttributeTypes() {
        return attributeTypes;
    }

    public int getTypeBitLen(){
        if(eventTypeBitLen == 0){
            int maxTypeNum  = (int) attributeMaxValues[0];
            int ans = 0;
            while(maxTypeNum != 0){
                ans++;
                maxTypeNum >>= 1;
            }
            eventTypeBitLen = ans;
            return ans;
        }else{
            return eventTypeBitLen;
        }
    }


    public boolean setAttributeValueRange(String statement){
        boolean flag = false;
        String[] words = statement.split(" ");
        String attrName = words[5];

        int l = words[8].length();
        String range = words[8].substring(1, l - 1);
        String[] values = range.split(",");
        String min = values[0];
        String max = values[1];

        System.out.println("attrName: " + attrName + " min value: " + min + " max value: " + max);
        for(int i = 0; i < attributeNames.length; i++){
            if(attrName.equals((attributeNames[i]))){
                attributeMaxValues[i] = Double.parseDouble(max);
                attributeMinValues[i] = Double.parseDouble(min);
                flag = true;
            }
        }
        return flag;
    }

    public int getAttributeId(String attrName){
        for(int i = 0; i < attributeNames.length; i++){
            if(attributeNames[i].equals(attrName)){
                return i;
            }
        }
        return -1;
    }

    public double getIthAttrMaxValue(int ith){
        return attributeMaxValues[ith];
    }

    public double getIthAttrMinValue(int ith){
        return attributeMinValues[ith];
    }

    public String getIthAttrType(int ith){
        return attributeTypes[ith];
    }




    public int getMaxEventTypeNum(){
        int ans = (int) attributeMaxValues[0];
        if(ans == Integer.MAX_VALUE){
            return -1;
        }else{
            return ans;
        }

    }

    public String getAttributeType(String attrName){
        for(int i = 0; i < attributeNames.length; i++){
            if(attributeNames[i].equals(attrName)){
                return attributeTypes[i];
            }
        }
        System.out.println("This schema do not have a attrName: \'" + attrName + "\'");
        return null;
    }

    /*print schema information*/
    public void print(){

        System.out.println("schema_name: \'" +  schemaName + "\'");
        System.out.println("-------------------------------------------------------------------------------------");
        System.out.print(String.format("|%-16s|", "Attribute name"));
        System.out.print(String.format("%-16s|", "Attribute type"));
        System.out.print(String.format("%-24s|", "Minimum value"));
        System.out.println(String.format("%-24s|", "Maximum value"));
        System.out.println("-------------------------------------------------------------------------------------");
        for(int i = 0; i < attributeNames.length; i++){
            System.out.print(String.format("|%-16s|", attributeNames[i]));
            System.out.print(String.format("%-16s|", attributeTypes[i]));
            System.out.print(String.format("%-24s|", attributeMinValues[i]));
            System.out.println(String.format("%-24s|", attributeMaxValues[i]));
        }
        System.out.println("-------------------------------------------------------------------------------------");
    }

    public void printEventTypeMap(){
        for(Map.Entry entry : eventTypeMap.entrySet()){
            String mapKey = (String) entry.getKey();
            int mapValue = (Integer) entry.getValue();
            System.out.println("Key: \'" + mapKey+"\'" + " value: " + mapValue);
        }
    }

    public static void main(String[] args){
        OldEventSchema s = new OldEventSchema("CREATE SCHEMA stock (ticker TYPE, open FLOAT, volume INT, time TIMESTAMP)");
        s.setAttributeValueRange("ALTER TABLE stock ADD CONSTRAINT ticker IN RANGE [1,5000]");
        s.setAttributeValueRange("ALTER TABLE stock ADD CONSTRAINT open IN RANGE [0,1000]");
        s.setAttributeValueRange("ALTER TABLE stock ADD CONSTRAINT volume IN RANGE [0,100000]");
        s.print();
    }
}
