package Common;

import java.util.Arrays;

/**
 * Transformer用来合并ts-left和event_type的值
 * 主要用来解决Java中没有结构体这件事情
 */
public class KeyTransformer {
    public static int splitPos;
    public static byte[] generateKey(long tsLeft, int bitLen1, int eventType, int bitLen2){
        splitPos = bitLen2;
        int sumBitSum = bitLen1 + bitLen2;
        int needByteNum;

        if(sumBitSum % 8 == 0){
            needByteNum = sumBitSum >> 3;
        }else{
            needByteNum = (sumBitSum >> 3) + 1;
        }

        byte[] combineKey = new byte[needByteNum];
        int index = 0;
        byte mask = (byte) 255;
        while(bitLen2 > 8){
            byte b = (byte) (eventType & mask);
            combineKey[index++] = b;
            eventType >>= 8;
            bitLen2 -= 8;
        }

        int left = 8 - bitLen2;
        if(bitLen1 > left){
            combineKey[index++] = (byte) ((((mask >> bitLen2) & tsLeft) << bitLen2 )+ eventType);
        }
        tsLeft >>= left;

        for(int i = index; i < needByteNum; ++i){
            combineKey[i] = (byte) (tsLeft & mask);
            tsLeft >>= 8;
        }
        return combineKey;
    }

    public int getSplitPos() {
        return splitPos;
    }

    public void setSplitPos(int splitPos) {
        this.splitPos = splitPos;
    }

    public long getTsLeft(byte[] combineKey){
        long ans = 0;
        int end = splitPos % 8 == 0 ? splitPos / 8 : splitPos/ 8 + 1;
        for(int i = combineKey.length - 1; i >= end; --i){
            ans <<= 8;
            ans += Byte.toUnsignedLong(combineKey[i]);
        }
        if(splitPos % 8 != 0){
            ans <<= (8 - splitPos % 8);
            long rear = Byte.toUnsignedLong(combineKey[end - 1]) >> (splitPos % 8);
            ans  = ans + rear;
        }
        return ans;
    }

    public static int getEventType(byte[] combineKey){
        int ans = 0;
        int start = splitPos / 8 - 1;
        if(splitPos % 8 != 0){
            int low = 0x00ff >> (8 - splitPos % 8);
            ans = Byte.toUnsignedInt(combineKey[start + 1]) & low;
        }
        for(int i = start; i >= 0; --i){
            ans <<= 8;
            ans += Byte.toUnsignedInt(combineKey[i]);
        }

        return ans;
    }

    public boolean equals(byte[] x, byte[] y){
        if(x.length != y.length){
            return false;
        }else{
            for(int i = 0; i < x.length; i++){
                if(x[i] != y[i]){
                    return false;
                }
            }
        }
        return true;
    }

    public int getHashCode(byte[] x){
        return Arrays.hashCode(x);
    }


    public void print(byte[] combineKey){
        for(int i = combineKey.length - 1; i >= 0; --i){
            System.out.println(i + "-th byte value: " + Byte.toUnsignedInt(combineKey[i]));
        }
    }
}
