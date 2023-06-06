package Common;

public class HashKey {
    /*
    key是由时间戳的左部分+事件类型组成
     */
    private byte[] combineKey;
    private short splitPos;

    /*
    tsLeft:     0001 1010 0100 0111 -> 13
    eventType:  0000 1110           -> 5
     0000 0011 0100 1000 1110 1110
    */
    public HashKey(long tsLeft, int bitLen1, int eventType, int bitLen2){
        splitPos = (short) bitLen2;
        int sumBitSum = bitLen1 + bitLen2;
        int needByteNum;

        if((sumBitSum & 0x0f) == 0){
            needByteNum = sumBitSum >> 3;
        }else{
            needByteNum = (sumBitSum >> 3) + 1;
        }

        combineKey = new byte[needByteNum];
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
    }

    public long getTsLeft(){
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

    public int getEventType(){
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

    public void print(){
        System.out.println("HashKey: ");
        for(int i = combineKey.length - 1; i >= 0; --i){
            System.out.println(i + "-th byte value: " + Byte.toUnsignedInt(combineKey[i]));
        }
    }

    @Override
    public int hashCode() {
        int hashValue = 0;

        int byteStart = combineKey.length <= 4 ? combineKey.length - 1 : 3;
        for(int i = byteStart; i >= 0; --i){
            hashValue <<= 8;
            hashValue += combineKey[i];
        }

        return hashValue;
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null || !(obj instanceof HashKey)){
            return false;
        }else{
            return ((HashKey) obj).getEventType() == this.getEventType()
                && ((HashKey) obj).getTsLeft() == this.getTsLeft();
        }
    }

    public static void main(String[] args){

        //1110 0001 1111 0001 1010 1110 0001 1111
        long[] tsLeftTest = {6727, 25073, 925466};
        short[] len1Test = {13, 15, 20};

        int[] eventTypeTest = {14, 133, 3615};
        short[] len2Test = {5, 8, 12};

        for(int i = 0; i < tsLeftTest.length; i++){
            HashKey key = new HashKey(tsLeftTest[i], len1Test[i], eventTypeTest[i], len2Test[i]);
            key.print();
            System.out.println("tsLeft real value: " + tsLeftTest[i] + " recovery value: " + key.getTsLeft());
            System.out.println("eventType real value: " + eventTypeTest[i] + " recovery value: " + key.getEventType());
        }
    }
}
