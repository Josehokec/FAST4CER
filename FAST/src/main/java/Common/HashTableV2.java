package Common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashTableV2 {
    private int perAttrUsedBit;
    private int attrStoredNum;
    private HashMap<byte[], List<byte[]>> hashTable;


    public HashTableV2(){
        hashTable = new HashMap<>();
    }

    public HashTableV2(int perAttrUsedBit, int attrStoredNum) {
        this.perAttrUsedBit = perAttrUsedBit;
        this.attrStoredNum = attrStoredNum;
    }

    /*
    r应该小于1的,V是属性向量使用的bit数量
     */
    public long calSpaceOverhead(OldEventSchema s, float r, int L, int V){
        int C = 64;
        int R = C - L;
        int T = s.getTypeBitLen();
        long ans = (long) ((((L+T) >> 3) + ((L + T) & 7)) * (2 << L) + (2 << R) * r * ((R>>3) + (R & 7)+(V >> 3)));
        return ans;
    }

    /*
    计算时间戳高低位划分的最优参数设置


     */

    /**
     * 计算时间戳高低位划分的最优参数设置
     * 注意到C=64，r是平均的事件到达率
     * @param s EventSchema Object
     * @param r event arrival rate
     * @return optimal left value
     */
    public int getOptimalLeft(OldEventSchema s, float r){
        int V = attrStoredNum * perAttrUsedBit;
        int C = 64;
        int ans = -1;
        if(s.getTypeBitLen() >= (r * (2 << C) - 1) / (Math.log(2))){
            System.out.println("TsLeft len should be be set zero.");
            ans = 0;
        }else{
            long min = Long.MAX_VALUE;
            for(int i = 1; i < 64; i++){
                long o = calSpaceOverhead(s, r, i, V);
                if(min > o){
                    ans = i;
                    min = o;
                }
            }
        }
        return ans;
    }

    /**
     * byte[] 的hashCode如何重写？？？
     * @param key
     * @param value
     */
    public void insertElement(byte[] key, byte[] value){
        if(hashTable.containsKey(key)){
            List<byte[]> list = hashTable.get(key);
            list.add(value);
        }else{
            List<byte[]> list = new ArrayList<>();
            list.add(value);
            hashTable.put(key, list);
        }
    }
}
