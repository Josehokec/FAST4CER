package Common;

import java.util.List;

/**
 * 默认fpVector已经是8的倍数了
 */
public class HashValue {
    /*
    HashValue是由时间戳的右部（低位）和每个属性的指纹组成
    值等值比较函数，值范围比较函数
     */
    private byte[] tsRight;
    private byte[] attrFPVector;

    /*
    时间戳右部的值，时间戳右部所占bit数量，属性指纹向量
     */
    public HashValue(long tsR, short len, byte[] fpVector){
        int needByteNum ;
        if(len % 8 == 0){
            needByteNum = len / 8;
        }else{
            needByteNum = len / 8 + 1;
        }

        this.tsRight = new byte[needByteNum];
        for(int i = 0; i < needByteNum; ++i){
            tsRight[i] = (byte) (tsR & 0xff);
            tsR >>= 8;
        }

        this.attrFPVector = new byte[fpVector.length];
        System.arraycopy(fpVector, 0, attrFPVector, 0, fpVector.length);
    }

    public long getTsRight(){
        long ans = 0;
        for(int i = tsRight.length - 1; i >= 0; --i){
            ans <<= 8;
            ans += Byte.toUnsignedLong(tsRight[i]);
        }
        return ans;
    }

    /*
    得到属性向量中各个属性代表的值
    bitNum一般取6，8，10，12，14，16比较好
     */

    /**
     * 得到属性向量中各个属性代表的值
     * 假设每个事件属性使用8bit存储，一共有两个属性存，那么属性指纹向量需要使用16bit
     * @param bitNum
     * @return
     */
    public int[] getAttributeFPValue(int bitNum){
        // bitNum = 4, attrFPVector.length * 8 = 16, 但实际上只有3个属性，到时候会多出一个，也没关系
        int attributeNum = attrFPVector.length * 8 / bitNum;
        int[] ans = new int[attributeNum];
        if(bitNum == 4){
            for(int i = 0; i < attributeNum; i = i + 2){
                ans[i] = attrFPVector[i] & 0x0f;
                ans[i + 1] = (attrFPVector[i] & 0x0f0) >> 4;
            }
        }else if(bitNum == 8){
            for(int i = 0; i < attributeNum; ++i){
                ans[i] = Byte.toUnsignedInt(attrFPVector[i]);
            }
        }else if(bitNum == 12){
            for(int i = 0; i < attributeNum; i = i + 3){
                int pos = i * 2 / 3;
                ans[pos++] = ((attrFPVector[i + 1] & 0x0f) << 8) + (attrFPVector[i] & 0x0ff);// 低8位 + 高4位
                if(i + 1 < attributeNum){
                    ans[pos] = ((attrFPVector[i + 1] & 0x0f0) >> 4) + ((attrFPVector[i + 2] & 0x0ff) << 4); // 低4位 + 高8位
                }
            }
        }else if(bitNum == 16){
            for(int i = 0; i < attributeNum; i = i + 2){
                ans[i >> 1] = ((attrFPVector[i + 1] & 0x0ff) << 8) + (attrFPVector[i] & 0x0ff);
            }
        }

        return ans;
    }

    /*
    查看第i个属性值是不是在一个范围之间
     */
    public boolean include(int ith, int bitNum, int min, int max){
        int[] fpValue = getAttributeFPValue(bitNum);
        if(fpValue[ith] <= max && fpValue[ith] >= min){
            return true;
        }else{
            return false;
        }
    }



    public void print(){
        System.out.println("HashValue ts-right: ");
        for(int i = tsRight.length - 1; i >= 0; --i){
            System.out.print(i + "-th byte value: " + Byte.toUnsignedInt(tsRight[i]) +"\t\t");
        }
        System.out.println("\nHashValue attrFPVector: ");
        for(int i = 0; i < attrFPVector.length; i++){
            System.out.print(i + "-th byte value: " + Byte.toUnsignedInt(attrFPVector[i]) + "\t\t");
        }
        System.out.println();
    }
}
