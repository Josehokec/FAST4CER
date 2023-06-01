package RTree;

public class MyPoint implements Cloneable{
    /* for event entry
    data[0] is event type
    data[1: data.length) is event attributes
    timestamp is event occur timestamp
    后续为了省空间，需要将event type和attribute分开，每个attribute开一个字节就够了
    event type则根据具体有多少个事件类型选择开多大的空间
    如果有一个属性是类别型的，且类别比较少，建议构造bitmap索引
     */
    private final int[] data;
    private long timestamp;

    public MyPoint(int[] data) {
        // must use copy function
        if(data == null) {
            throw new IllegalArgumentException("Point is null");
        }else if(data.length == 1){
            throw new IllegalArgumentException("Point dimension should be >= 2");
        }

        this.data = new int[data.length];
        // System.arraycopy() is quickest, clone() is most low
        System.arraycopy(data, 0, this.data, 0, data.length);
    }

    public MyPoint(int[] data, long timestamp){
        if(data == null) {
            throw new IllegalArgumentException("Point is null");
        }else if(data.length == 1){
            throw new IllegalArgumentException("Point dimension should be >= 2");
        }

        this.data = new int[data.length];
        // System.arraycopy() is quickest, clone() is most low
        System.arraycopy(data, 0, this.data, 0, data.length);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getPosValue(int pos) {
        if(pos < 0 || pos >= data.length){
            throw new IllegalArgumentException("index out of bounded");
        }
        return data[pos];
    }

    public void setPosValue(int pos, int value){
        if(pos < 0 || pos >= data.length){
            throw new IllegalArgumentException("index out of bounded");
        }
        data[pos] = value;
    }

    public int getDimension(){
        // return the data dimension
        return data.length;
    }

    public boolean equals(Object obj){
        if(obj instanceof MyPoint){
            MyPoint p = (MyPoint) obj;
            if(p.getDimension() != data.length){
                throw new IllegalArgumentException("Two points have different dimension");
            }
            for(int i = 0; i < data.length; i++){
                if(data[i] != p.getPosValue(i)){
                    return false;
                }
            }
        }else{
            return false;
        }
        return true;
    }

    @Override
    protected Object clone() {
        int [] copy = new int[data.length];
        System.arraycopy(data, 0, copy, 0, data.length);
        return new MyPoint(copy);
    }

    @Override
    public String toString(){
        // StringBuffer is thread safe but slow
        // StringBuilder is fast but thread unsafe
        StringBuilder ans= new StringBuilder("(");
        for(int i = 0; i < data.length; i++){
            if(i == data.length - 1){
                ans.append(data[i]).append(")");

            }else{
                ans.append(data[i]).append(", ");
            }
        }
        return ans.toString();
    }

    public static void main(String[] args){
        int[] test = {1,2,3,4};
        MyPoint p = new MyPoint(test);

        test[0] = 9;
        System.out.println(p);
        System.out.println("Point dimension: " + p.getDimension());

        p.setPosValue(0, 8);
        System.out.println("After setPos(0,8), new p " + p);
        System.out.println("Point[2] is :" + p.getPosValue(2));

        MyPoint q = (MyPoint) p.clone();
        System.out.println("p equals q? " + p.equals(q));

        q.setPosValue(0,7);
        System.out.println("After setPos(0,7), new q " + q);
        System.out.println("After setPos(0,7), old p " + p);
        System.out.println("p equals q? " + p.equals(q));

        int[] diffTest = {1, 2, 3};
        MyPoint r = new MyPoint(diffTest);
        System.out.println("r equals q? " + r.equals(q));
    }
}
