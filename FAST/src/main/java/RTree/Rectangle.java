package RTree;

public class Rectangle {
    // Once a final variable has been initialized and assigned
    // it cannot be assigned again.
    private final MyPoint leftBottom;
    private final MyPoint rightTop;

    public Rectangle(MyPoint leftBottom, MyPoint rightTop){

        // Point objects cannot be null and have same dimension
        if(leftBottom == null || rightTop == null){
            throw new IllegalArgumentException("Point cannot be null");
        }else if(leftBottom.getDimension() != rightTop.getDimension()){
            throw new IllegalArgumentException("Dimension of Points are different");
        }

        // the leftBottom value of all dimension is less than rightTop
        for(int i = 0; i < leftBottom.getDimension(); i++){
            if(leftBottom.getPosValue(i) > rightTop.getPosValue(i)){
                throw new IllegalArgumentException("cannot satisfy leftBottom.value <= rightTop.value");
            }
        }

        this.leftBottom = leftBottom;
        this.rightTop = rightTop;
    }

    public MyPoint getLeftBottom() {
        return (MyPoint) leftBottom.clone();
    }

    public MyPoint getRightTop() {
        return (MyPoint) rightTop.clone();
    }

    public int getDimension(){
        return leftBottom.getDimension();
    }

    public int getArea(){
        int ans = 1;
        for(int i = 0; i < getDimension(); i++){
            ans *= (rightTop.getPosValue(i) - leftBottom.getPosValue(i));
        }
        return ans;
    }

    public Rectangle unionRectangle(Rectangle rectangle){
        // choose a more large Rectangle to include rectangles and itself
        if(rectangle == null){
            throw new IllegalArgumentException("rectangle cannot be null");
        }else if(rectangle.getDimension() != this.getDimension()){
            throw new IllegalArgumentException("two rectangles don't have same dimension");
        }

        int dimension = leftBottom.getDimension();
        int[] min = new int[dimension];
        int[] max = new int[dimension];

        for(int i = 0; i < dimension; i++){
            min[i] = Math.min(leftBottom.getPosValue(i), rectangle.leftBottom.getPosValue(i));
            max[i] = Math.max(rightTop.getPosValue(i), rectangle.rightTop.getPosValue(i));
        }

        return new Rectangle(new MyPoint(min), new MyPoint(max));
    }

    public Rectangle unionMoreRectangle(Rectangle[] rectangles){
        // note: this function don't union itself
        if(rectangles == null || rectangles.length == 0){
            throw new IllegalArgumentException("rectangles cannot be null");
        }

        Rectangle R0 = (Rectangle) rectangles[0].clone();
        for(int i = 1; i < rectangles.length; i++){
            R0 = R0.unionRectangle(rectangles[i]);
        }

        return R0;
    }

    public int intersectArea(Rectangle rectangle){
        if(!isIntersection(rectangle)){
            return 0;
        }
        int area = 1;
        for(int i = 0; i < getDimension(); i++){
            int l1 = this.leftBottom.getPosValue(i);
            int h1 = this.rightTop.getPosValue(i);
            int l2 = rectangle.leftBottom.getPosValue(i);
            int h2 = rectangle.rightTop.getPosValue(i);
            /*
            [1, 1] [3, 4]
            [2, 3] [5, 5]
             */
            // because two rectangles intersect
            if(l1 <= l2 && h1 <= h2){
                area *= (h1 - l2);
            }else if(l1 >= l2 && h1 >= h2){
                area *= (h2 - l1);
            }else if(l1 <= l2 && h1 >= h2){
                area *= (h2 - l2);
            }else if(l1 >= l2 && h1 <= h2){
                area *= (h1 - l1);
            }
        }

        return area;
    }

    public boolean isIntersection(Rectangle rectangle){
        // judge two rectangles whether intersection
        // return yes -> intersection
        if(rectangle == null){
            throw new IllegalArgumentException("rectangle cannot be null");
        }else if(rectangle.getDimension() != this.getDimension()){
            throw new IllegalArgumentException("two rectangles don't have same dimension");
        }

        for(int i = 0; i < getDimension(); i++){
            if(leftBottom.getPosValue(i) >= rectangle.rightTop.getPosValue(i) ||
                    rightTop.getPosValue(i) <= rectangle.leftBottom.getPosValue(i)){
                continue;
            }else{
                return true;
            }
        }

        return leftBottom.equals(rectangle.rightTop) || rightTop.equals(rectangle.leftBottom);
    }

    public boolean enclosure(Rectangle rectangle){
        // judge this.rectangle whether includes rectangle
        // this.rectangle is larger
        if(rectangle == null){
            throw new IllegalArgumentException("rectangle cannot be null");
        }else if(rectangle.getDimension() != this.getDimension()){
            throw new IllegalArgumentException("two rectangles don't have same dimension");
        }

        for(int i = 0; i < getDimension(); i++){
            // rightTop is private, why rectangle can get it?
            if(rectangle.leftBottom.getPosValue(i) < leftBottom.getPosValue(i) ||
                    rectangle.rightTop.getPosValue(i) > rightTop.getPosValue(i)){
                return false;
            }
        }

        return true;
    }

    public boolean containPoint(MyPoint point){
        // judge this rectangle whether contain a point
        if(point == null){
            throw new IllegalArgumentException("Point cannot be null");
        }else if(point.getDimension() != this.getDimension()){
            throw new IllegalArgumentException("Dimension of Points are different");
        }
        for(int i = 0; i < getDimension(); i++){
            if(point.getPosValue(i) < leftBottom.getPosValue(i) ||
                    point.getPosValue(i) > rightTop.getPosValue(i)){
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof  Rectangle){
            Rectangle rectangle = (Rectangle) obj;
            return leftBottom.equals(rectangle.getLeftBottom()) && rightTop.equals(rectangle.getRightTop());
        }
        return false;
    }

    @Override
    protected Object clone(){
        MyPoint newLeftBottom = (MyPoint) leftBottom.clone();
        MyPoint newRightTop = (MyPoint) rightTop.clone();
        return new Rectangle(newLeftBottom, newRightTop);
    }

    @Override
    public String toString(){
        StringBuilder ans= new StringBuilder();
        ans.append("Rectangle leftBottom: ").append(leftBottom);
        ans.append(" rightTop: ").append(rightTop);
        return ans.toString();
    }

    public static void main(String[] args){
        int[] p1 = {1, 1};
        int[] p2 = {3, 4};
        int[] p3 = {2, 3};
        int[] p4 = {5, 5};
        int[] p5 = {0, 0};
        int[] p6 = {5, 5};
        int[] p7 = {4, 4};
        int[] p8 = {6, 6};
        int[] p9 = {3, 4};
        int[] p10 = {7, 7};
        Rectangle R1 = new Rectangle(new MyPoint(p1), new MyPoint(p2));
        Rectangle R2 = new Rectangle(new MyPoint(p3), new MyPoint(p4));
        Rectangle R3 = new Rectangle(new MyPoint(p5), new MyPoint(p6));
        Rectangle R4 = new Rectangle(new MyPoint(p7), new MyPoint(p8));
        Rectangle R5 = new Rectangle(new MyPoint(p9), new MyPoint(p10));
        Rectangle[] RArray1 = {R1, R2, R3, R4};
        Rectangle[] RArray2 = {R1, R2, R4};

        System.out.println("R1: " + R1);
        System.out.println("R2: " + R2);
        System.out.println("R3: " + R3);
        System.out.println("R4: " + R4);
        System.out.println("R3 equals R2? " + R3.equals(R2));
        //测试union函数
        Rectangle newR = R2.unionRectangle(R1);
        System.out.println("R1 union R2: " + newR);

        // 测试相交函数
        System.out.println("R1 and R2 intersect? " + R1.isIntersection(R2));
        System.out.println("R1 and R3 intersect? " + R1.isIntersection(R3));
        System.out.println("R2 and R4 intersect? " + R2.isIntersection(R4));
        System.out.println("R1 and R4 intersect? " + R1.isIntersection(R4));
        System.out.println("R4 and R1 intersect? " + R4.isIntersection(R1));
        System.out.println("R1 and R5 intersect? " + R1.isIntersection(R5));

        System.out.println("Union All Rectangle:  " + R1.unionMoreRectangle(RArray1));
        System.out.println("Union All Rectangle:  " + R1.unionMoreRectangle(RArray2));

        System.out.println("R3 include R2? " + R3.enclosure(R2));
        System.out.println("R3 include R1? " + R3.enclosure(R1));
        System.out.println("R1 include R2? " + R1.enclosure(R2));
        System.out.println("R1 include R4? " + R1.enclosure(R4));
        System.out.println("R1 include R4? " + R1.enclosure(R4));

        // 测试面积函数
        System.out.println("R1 area: " + R1.getArea());
        System.out.println("R2 area: " + R2.getArea());
        System.out.println("R3 area: " + R3.getArea());

        // 测试相交函数
        System.out.println("R1 and R2 intersection area: " + R1.intersectArea(R2));
        System.out.println("R1 and R3 intersection area: " + R1.intersectArea(R3));
        System.out.println("R3 and R5 intersection area: " + R3.intersectArea(R5));
        System.out.println("R1 and R4 intersection area: " + R1.intersectArea(R4));
        System.out.println("R1 and R5 intersection area: " + R1.intersectArea(R5));

        // 测试包含点的函数
        MyPoint testP0 = new MyPoint(new int[]{0, 0});
        MyPoint testP1 = new MyPoint(new int[]{1, 1});
        MyPoint testP7 = new MyPoint(new int[]{4, 4});
        System.out.println("R1 contains testP0: " + R1.containPoint(testP0));
        System.out.println("R1 contains testP1: " + R1.containPoint(testP1));
        System.out.println("R1 contains testP4: " + R1.containPoint(testP7));
    }

}
