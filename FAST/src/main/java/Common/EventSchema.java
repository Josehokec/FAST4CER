package Common;

import Store.EventStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * attNameMap用来找到属性名字是存在哪个位置
 * 比如: ticker TYPE, open FLOAT.2, volume INT, time TIMESTAMP
 * attNameMap.get(ticker) = 0; attNameMap.get(volume) = 2;attNameMap.get(time) = 3;
 * typeMap是用来把字符串类型的event type转换成整型的
 * 注意最大最小值是转换过的
 */
public class EventSchema {
    private int hasAssignedId;                          // 给事件类型分配的ID
    private int maxEventTypeBitLen;                     // 事件类型所需要的bit数量
    private int storeRecordSize;                        // 存储一条记录所需要的字节数量
    private String schemaName;                          // schema名字
    private String[] attrNames;                         // 属性名字
    private String[] attrTypes;                         // 属性类型
    private int[] decimalLens;                          // 标记保留几位小数点
    private StorePos[] positions;                       // 保存存储的位置
    private long[] attrMinValues;                       // 每个属性的最小值
    private long[] attrMaxValues;                       // 每个属性的最大值
    private final List<String> allEventTypes;           // typeId 对于的属性事件类型
    private EventStore store;                           // 这个事件的存储
    private final HashMap<String, Integer> attrNameMap; // attNameMap用来找到属性名字是存在哪个位置
    private final HashMap<String, Integer> typeMap;     //event type映射map，把字符串变成整型

    public EventSchema() {
        attrNameMap = new HashMap<>();
        typeMap = new HashMap<>();
        hasAssignedId = 0;
        maxEventTypeBitLen = -1;
        storeRecordSize = -1;
        allEventTypes = new ArrayList<>();
        // Because we assign IDs starting from 1, we add null
        allEventTypes.add("null");
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    public int getStoreRecordSize() {
        return storeRecordSize;
    }

    public EventStore getStore() {
        return store;
    }

    public void setStore(EventStore store) {
        this.store = store;
    }

    public String[] getAttrNames() {
        return attrNames;
    }

    public void setAttrNames(String[] attrNames) {
        this.attrNames = attrNames;
    }

    public String[] getAttrTypes() {
        return attrTypes;
    }

    /**
     * 设置属性名字之后 更新存储位置 以及存储记录占用的字节数量
     * @param attrTypes 属性类型名字
     */
    public void setAttrTypes(String[] attrTypes) {
        this.attrTypes = attrTypes;
        // 设置存储时候属性所占用的字节位置
        positions = new StorePos[attrTypes.length];
        int startPos = 0;
        //int占4个字节，Type占4个字节，timestamp占八个字节，float和double占8个字节
        for(int i = 0; i < attrTypes.length; ++i){
            if(attrTypes[i].equals("INT") || attrTypes[i].equals("TYPE")){
                positions[i] = new StorePos(startPos, 4);
                startPos += 4;
            }else if(attrTypes[i].contains("FLOAT") || attrTypes[i].contains("DOUBLE") || attrTypes[i].equals("TIMESTAMP")){
                positions[i] = new StorePos(startPos, 8);
                startPos += 8;
            }else{
                throw new RuntimeException("Do not support this type'" + attrTypes[i] + "'.");
            }
        }
        storeRecordSize = startPos;
        //System.out.println("recordSize: " + storeRecordSize);
    }

    public void setDecimalLens(int[] decimalLens) {
        this.decimalLens = decimalLens;
    }

    public void setAttrMinValues(long[] attrMinValues) {
        this.attrMinValues = attrMinValues;
    }

    public void setAttrMaxValues(long[] attrMaxValues) {
        this.attrMaxValues = attrMaxValues;
    }

    public void insertAttrName(String attrName, int idx){
        attrNameMap.put(attrName, idx);
    }

    public int getAttrNameIdx(String attrName){
        return attrNameMap.get(attrName);
    }

    public String getIthAttrType(int idx){
        return attrTypes[idx];
    }

    public long getIthAttrMinValue(int idx){
        return attrMinValues[idx];
    }

    public void setIthAttrMinValue(int idx, long minValue){
        attrMinValues[idx] = minValue;
    }

    public long getIthAttrMaxValue(int idx){
        return attrMaxValues[idx];
    }

    public void setIthAttrMaxValue(int idx, long minValue){
        attrMaxValues[idx] = minValue;
    }

    public int  getIthDecimalLens(int idx) {
        return decimalLens[idx];
    }

    /**
     * 如果这个事件类型之前存储过，那么就直接返回，否则需要分配一个id
     * @param eventType 字符串类型的事件类型
     * @return 分配的id
     */
    public int getTypeId(String eventType){
        if(typeMap.containsKey(eventType)){
            return typeMap.get(eventType);
        }else{
            allEventTypes.add(eventType);
            typeMap.put(eventType, ++hasAssignedId);
            return hasAssignedId;
        }
    }

    public int getTypeIdLen(){
        return maxEventTypeBitLen == -1 ? calTypeIdBitLen() : maxEventTypeBitLen;
    }

    public int getPageStoreRecordNum(){
        int pageSize = store.getPageSize();
        return pageSize / storeRecordSize;
    }

    public int getMaxEventTypeNum(){
        for(int i = 0; i< attrTypes.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                return (int) attrMaxValues[i];
            }
        }
        return -1;
    }

    public int calTypeIdBitLen(){
        int ans = 0;
        long maxEventTypeNum = Integer.MAX_VALUE;
        for(int i = 0; i< attrTypes.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                maxEventTypeNum = attrMaxValues[i];
                break;
            }
        }

        while(maxEventTypeNum != 0){
            maxEventTypeNum >>= 1;
            ans++;
        }
        maxEventTypeBitLen = ans;
        return ans;
    }

    public int getTimestampIdx(){
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("TIMESTAMP")){
                return i;
            }
        }
        throw new IllegalStateException("This schema is missing the timestamp attribute.");
    }

    /**
     * 得到schema中事件类型所在的位置
     * @return 位置
     */
    public int getTypeIdx(){
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("TYPE")){
                return i;
            }
        }
        throw new IllegalStateException("This schema is missing the timestamp attribute.");
    }

    /**
     * 把字符串类型的记录转换成字节类型的数组
     * 方便存储在文件中
     */
    public byte[] convertToBytes(String[] attrValues){

        if(storeRecordSize == -1){
            throw new RuntimeException("wrong state");
        }
        byte[] ans = new byte[storeRecordSize];
        int ptr = 0;
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("INT")){
                int value = Integer.parseInt(attrValues[i]);
                byte[] b = Converter.intToBytes(value);
                System.arraycopy(b, 0, ans, ptr, 4);
                ptr += 4;
            }else if(attrType.contains("FLOAT")){
                float value = Float.parseFloat(attrValues[i]);
                int magnification = (int) Math.pow(10, getIthDecimalLens(i));
                long newValue = (long) (value * magnification);
                byte[] b = Converter.longToBytes(newValue);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else if(attrType.contains("DOUBLE")){
                double value = Double.parseDouble(attrValues[i]);
                int magnification = (int) Math.pow(10, getIthDecimalLens(i));
                long newValue = (long) (value * magnification);
                byte[] b = Converter.longToBytes(newValue);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else if(attrType.contains("TYPE")){
                String eventType = attrValues[i];
                int type = getTypeId(eventType);
                byte[] b = Converter.intToBytes(type);
                System.arraycopy(b, 0, ans, ptr, 4);
                ptr += 4;
            }else if(attrType.equals("TIMESTAMP")){
                long timestamp = Long.parseLong(attrValues[i]);
                byte[] b = Converter.longToBytes(timestamp);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else{
                throw new RuntimeException("Do not support this type'" + attrType + "'.");
            }
        }
        if(storeRecordSize != ptr){
            throw new RuntimeException("convert has exception.");
        }
        return ans;
    }

    /**
     * 从字节记录中拿到事件类型
     * @param record    字节记录
     * @param typeIdx   类型所在的列编号
     * @return          该条记录对应事件类型
     */
    public final String getTypeFromBytesRecord(byte[] record, int typeIdx){
        int start = positions[typeIdx].startPos();
        int offset = positions[typeIdx].offset();
        byte[] bytes = new byte[offset];
        System.arraycopy(record, start, bytes, 0, offset);
        int v = Converter.bytesToInt(bytes);
        // convert int to string
        return allEventTypes.get(v);
    }

    public final long getValueFromBytesRecord(byte[] record, int colIdx){
        int start = positions[colIdx].startPos();
        int offset = positions[colIdx].offset();
        byte[] bytes = new byte[offset];
        System.arraycopy(record, start, bytes, 0, offset);
        return Converter.bytesToLong(bytes);
    }

    /**
     * 将字节数组转化成字符串记录
     * @param record 字节数组记录
     * @return 字符串记录
     */
    public String bytesToRecord(byte[] record){
        String ans = "";

        int ptr = 0;
        for(int i = 0; i < attrTypes.length; ++i) {
            String attrType = attrTypes[i];
            if (attrType.equals("INT")) {
                byte[] b = new byte[4];
                System.arraycopy(record, ptr, b, 0, 4);
                int v = Converter.bytesToInt(b);
                ans = (i == 0) ? (ans + v) : (ans + "," + v);
                ptr += 4;
            } else if (attrType.contains("FLOAT")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                float scale = (float) Math.pow(10, getIthDecimalLens(i));
                float rawValue = v / scale;
                ans = (i == 0) ? (ans + rawValue) : (ans + "," + rawValue);
                ptr += 8;
            } else if (attrType.contains("DOUBLE")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                double scale = Math.pow(10, getIthDecimalLens(i));
                double rawValue = v / scale;
                ans = (i == 0) ? (ans + rawValue) : (ans + "," + rawValue);
                ptr += 8;
            } else if (attrType.contains("TYPE")) {
                byte[] b = new byte[4];
                System.arraycopy(record, ptr, b, 0, 4);
                int v = Converter.bytesToInt(b);
                // 转换为字符串类型的
                String type = allEventTypes.get(v);
                ans = (i == 0) ? (ans + type) : (ans + "," + type);
                ptr += 4;
            } else if (attrType.equals("TIMESTAMP")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                ans = (i == 0) ? (ans + v) : (ans + "," + v);
                ptr += 8;
            } else {
                throw new RuntimeException("Do not support this type'" + attrType + "'.");
            }
        }
        return ans;
    }

    /**
     * 得到第i个属性对应的字节数组，后续根据类型转换成对应的变量
     * @param record 记录的字节数组
     * @param i 要查询的第i个属性
     * @return 第i个属性的字节数组
     */
    public byte[] getIthAttrBytes(byte[] record, int i){
        int start = positions[i].startPos();
        int len = positions[i].offset();
        byte[] ans = new byte[len];
        System.arraycopy(record, start, ans, 0, len);
        return ans;
    }

    public void print(){
        System.out.println("schema_name: '" +  schemaName + "'");
        System.out.println("-------------------------------------------------------------");
        System.out.printf("|%-12s|", "AttrName");
        System.out.printf("%-12s|", "AttrType");
        System.out.printf("%-16s|", "MinValue");
        System.out.printf("%-16s|%n", "MaxValue");
        System.out.println("-------------------------------------------------------------");

        for(int i = 0; i < attrNames.length; i++){
            System.out.printf("|%-12s|", attrNames[i]);
            System.out.printf("%-12s|", attrTypes[i]);
            System.out.printf("%-16s|", attrMinValues[i]);
            System.out.printf("%-16s|%n", attrMaxValues[i]);
        }
        System.out.println("-------------------------------------------------------------");

        typeMap.forEach((k, v) -> System.out.println("Event type: " + k + " type id: " + v));

    }

}
