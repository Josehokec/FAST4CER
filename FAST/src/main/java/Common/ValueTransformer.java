package Common;


public class ValueTransformer {
    public static int tsRightByteNum;
    public static int tsRightBitLen;
    public static int perAttrUsedBit;

    public static int attrNum;

    public ValueTransformer(){
        tsRightBitLen = 8;
        perAttrUsedBit = 8;
    }

    public ValueTransformer(int tsRightBitLen, int perAttrUsedBit, int attrNum){
        this.tsRightBitLen = tsRightBitLen;
        this.perAttrUsedBit = perAttrUsedBit;
        this.attrNum = attrNum;
    }

    public static void setPerAttrUsedBit(int perAttrUsedBit) {
        ValueTransformer.perAttrUsedBit = perAttrUsedBit;
    }

    public static void setTsRightBitLen(int tsRightBitLen) {
        ValueTransformer.tsRightBitLen = tsRightBitLen;
    }

    public byte[] generateValue(long tsR, byte[] fpVector){
        if(tsRightBitLen % 8 == 0){
            tsRightByteNum = tsRightBitLen / 8;
        }else{
            tsRightByteNum = tsRightBitLen / 8 + 1;
        }

        int needByteNum = fpVector.length + tsRightByteNum;

        byte[] ans = new byte[needByteNum];
        for(int i = 0; i < tsRightByteNum; ++i){
            ans[i] = (byte) (tsR & 0xff);
            tsR >>= 8;
        }

        System.arraycopy(fpVector, 0, ans, tsRightByteNum, fpVector.length);
        return ans;
    }

    public long getTsRight(byte[] value){
        long ans = 0;
        for(int i = tsRightByteNum - 1; i >= 0; --i){
            ans <<= 8;
            ans += Byte.toUnsignedLong(value[i]);
        }
        return ans;
    }

    public int[] getAttributeFPValue(byte[] value){
        int[] ans = new int[attrNum];
        int len = value.length;

        if(perAttrUsedBit == 4){
            if(attrNum % 2== 0){
                for(int i = tsRightByteNum, j = 0; i < len; ++i, ++j){
                    ans[2 * j] = value[i] & 0x0f;
                    ans[2 * j + 1] = (value[i] >> 4) & 0x0f;
                }
            }else{
                for(int i = tsRightByteNum, j = 0; i < len - 1; ++i, ++j){
                    ans[2 * j] = value[i] & 0x0f;
                    ans[2 * j + 1] = (value[i] >> 4) & 0x0f;
                }
                ans[attrNum - 1] = value[len - 1] & 0x0f;
            }
        }else if(perAttrUsedBit == 8){
            for(int i = tsRightByteNum; i < len; ++i){
                ans[i - tsRightByteNum] = Byte.toUnsignedInt(value[i]);
            }
        }else if(perAttrUsedBit == 12){
            if(attrNum % 2 == 0){
                for(int i = tsRightByteNum, j = 0; i < len; i = i + 3, j = j + 2){
                    ans[j] = ((value[i + 1] & 0x0f) << 8) + Byte.toUnsignedInt(value[i]);
                    ans[j + 1] = (Byte.toUnsignedInt(value[i + 2]) << 4) + ((value[i + 1] >> 4) & 0x0f);
                }
            }else{
                for(int i = tsRightByteNum, j = 0; i < len - 2; i = i + 3, j = j + 2){
                    ans[j] = ((value[i + 1] & 0x0f) << 8) + Byte.toUnsignedInt(value[i]);
                    ans[j + 1] = (Byte.toUnsignedInt(value[i + 2]) << 4) + ((value[i + 1] >> 4) & 0x0f);
                }
                ans[attrNum - 1] = ((value[len - 1] & 0x0f) << 8) + (value[len - 2] & 0x0ff);
            }
        }else if(perAttrUsedBit == 16){
            for(int i = tsRightByteNum; i < value.length; i = i + 2){
                ans[(i - tsRightByteNum) / 2] = (value[i + 1] << 8) + value[i];
            }
        }else if(value.length - tsRightByteNum <= 8){
            long convertNum = 0;
            for(int i = value.length - 1; i >= tsRightByteNum; --i){
                convertNum <<= 8;
                convertNum += Byte.toUnsignedLong(value[i]);
            }
            for(int i = 0; i < attrNum; i++){
                ans[i] = (int) (convertNum & (0x0ffff >> (16 - perAttrUsedBit)));
            }
        }else{
            System.out.println("Don't support perAttrUsedBit is:" + perAttrUsedBit);
        }

        return ans;
    }

    public boolean include(byte[] value, int ith, int min, int max){
        int[] fpValue = getAttributeFPValue(value);
        if(fpValue[ith] <= max && fpValue[ith] >= min){
            return true;
        }else{
            return false;
        }
    }

    public boolean leThreshold(byte[] value, int ith, int threshold){
        int[] fpValue = getAttributeFPValue(value);
        if(fpValue[ith] <= threshold){
            return true;
        }
        return false;
    }

    public boolean ltThreshold(byte[] value, int ith, int threshold){
        int[] fpValue = getAttributeFPValue(value);
        if(fpValue[ith] < threshold){
            return true;
        }
        return false;
    }

    public boolean geThreshold(byte[] value, int ith, int threshold){
        int[] fpValue = getAttributeFPValue(value);
        if(fpValue[ith] >= threshold){
            return true;
        }
        return false;
    }

    public boolean gtThreshold(byte[] value, int ith, int threshold){
        int[] fpValue = getAttributeFPValue(value);
        if(fpValue[ith] > threshold){
            return true;
        }
        return false;
    }

}
