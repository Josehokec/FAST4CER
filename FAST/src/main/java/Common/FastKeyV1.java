package Common;

import java.util.Arrays;
/**
 * [0,split) 是teLeft
 */
public class FastKeyV1 {
    private final byte[] key;
    private final int split;

    /**
     * 这里把这两个部分都设置成了8的倍数<br>
     * 比如tsLeftLen=10时，本来之需要10bit来存的，但实际存储用了16bit<br>
     * 如果希望更省空间则可以使用HashKey，但是其在译码效率上不如FastKey
     * @param tsLeft 时间戳高位
     * @param tsLeftLen 时间戳高位bit长度
     * @param typeId 类型编号
     * @param typeIdLen 类型长度
     */
    public FastKeyV1(long tsLeft, int tsLeftLen, int typeId, int typeIdLen){
        //需要的总的字节数
        int sumByte;
        // 最后三位如果都是0，说明是8的倍数
        if((tsLeftLen & 0x07) == 0){
            split = (tsLeftLen >> 3);
        }else{
            split = (tsLeftLen >> 3) + 1;
        }

        if((typeIdLen & 0x07) == 0){
            sumByte = split + (typeIdLen >> 3);
        }else{
            sumByte = split + (typeIdLen >> 3) + 1;
        }

        key = new byte[sumByte];

        for(int i = 0; i < split; ++i){
            key[i] = (byte) (tsLeft & 0xff);
            tsLeft >>= 8;
        }

        for(int i = split; i < sumByte; ++i){
            key[i] = (byte) (typeId & 0xff);
            typeId >>= 8;
        }
    }

    public byte[] getKey() {
        return key;
    }

    public int getSplit() {
        return split;
    }

    public long getTsLeft(){
        long ans = 0;
        for(int i = split - 1; i >= 0; --i){
            ans <<= 8;
            ans += (0x0ff & key[i]);
        }
        return ans;
    }

    public int geyTypeId(){
        int ans = 0;
        for(int i = key.length - 1; i >= split; --i){
            ans <<= 8;
            ans += (0x0ff & key[i]);
        }
        return ans;
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(key);
    }

    @Override
    public boolean equals(Object o){
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        FastKeyV1 k = (FastKeyV1) o;
        return Arrays.equals(key, k.getKey());
    }

    public void print(){
        System.out.println("TsLeft: " + getTsLeft() + " type id: " + geyTypeId());
    }

    public static void main(String[] args){
        FastKeyV1 key1 = new FastKeyV1(0x1A47, 16, 0x0e, 10);
        System.out.println("key1 tsLeft: " + key1.getTsLeft() + " typeId: " + key1.geyTypeId() + " hashCode: " + key1.hashCode());

        FastKeyV1 key2 = new FastKeyV1(0x1A47, 16, 0x0e, 10);
        System.out.println("key2 tsLeft: " + key2.getTsLeft() + " typeId: " + key2.geyTypeId() + " hashCode: " + key2.hashCode());

        FastKeyV1 key3 = new FastKeyV1(0x61f1, 15, 0x85, 9);
        System.out.println("key1 tsLeft: " + key3.getTsLeft() + " typeId: " + key3.geyTypeId() + " hashCode: " + key3.hashCode());

        FastKeyV1 key4 = new FastKeyV1(0xe1f1a, 20, 0xe1f, 12);
        System.out.println("key4 tsLeft: " + key4.getTsLeft() + " typeId: " + key4.geyTypeId() + " hashCode: " + key4.hashCode());


        System.out.println("key1 equals key2? " + key1.equals(key2));
        System.out.println("key3 equals key2? " + key3.equals(key2));
    }


}
