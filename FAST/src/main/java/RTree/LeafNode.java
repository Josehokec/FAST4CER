package RTree;

import java.util.Vector;

public class LeafNode extends RTreeNode {
    /*
    point是近似的点，是否需要存储真实的事件元组根据情况而定
    一个叶子节点存储的元素数量根据构造参数而定，这里认为叶子节点和非叶子节点数组大小相同
     */
    public MyPoint[] pointArray;
    public int insertPos;

    public LeafNode(){
        super(NodeType.LEAF_NODE);
        pointArray = new MyPoint[ArgsConfig.MAX_ENTRIES_IN_NODE + 1];
        insertPos = 0;
    }

    public LeafNode(RTreeNode father){
        super(NodeType.LEAF_NODE, father, 0);
        pointArray = new MyPoint[ArgsConfig.MAX_ENTRIES_IN_NODE + 1];
        insertPos = 0;
    }

    public Rectangle getMBR(){
        // 根据叶子节点的数据点，得到最小包围矩形
        // 如果单个单个插入元素，不推荐调用这个函数，因为复杂度是insertPos * dimension
        if(insertPos == 0){
            throw new IllegalArgumentException("this is null node, mbr is null");
        }
        int dimension = pointArray[0].getDimension();
        int[] min = new int[dimension];
        int[] max = new int[dimension];

        for(int i = 0; i < insertPos; i++){
            for(int j = 0; j < dimension; j++){
                if(i == 0){
                    min[j] = pointArray[0].getPosValue(j);
                    max[j] = pointArray[0].getPosValue(j);
                }else{
                    min[j] = Math.min(min[i], pointArray[i].getPosValue(j));
                    max[j] = Math.max(max[i], pointArray[i].getPosValue(j));
                }
            }
        }
        return new Rectangle(new MyPoint(min), new MyPoint(max));
    }

    public boolean insertPoint(MyPoint point){
        /*
        在叶子节点插入一条数据，若插入数据之后超过节点容量则需要分裂节点
        最开始是异常情况处理
         */
        if(point == null){
            throw new IllegalArgumentException("cannot insert null point");
        }

        if(insertPos < ArgsConfig.MAX_ENTRIES_IN_NODE) {
            // 如果这个节点还能够存数据，那么就将这个数据存到该节点中，可能还需要更新父亲节点的边界
            pointArray[insertPos++] = point;
            InternalNode father = (InternalNode) getFather();
            if (father != null) {
                //从底向上更新内部节点的边界，直到更新到根节点为止
                father.updateBoundary();
            }
            return true;
        }else{
            // 超过了叶子节点容量，需要分裂成两个叶子节点，然后把新的叶子节点
            LeafNode[] leafNodes = splitNode(point);
            LeafNode leftLeafNode = leafNodes[0];
            LeafNode rightLeafNode = leafNodes[1];

            // 得到这个叶子节点父亲，然后在这个父亲节点中加入两个叶子节点
            InternalNode father = (InternalNode) getFather();
            father.updateEntries(leftLeafNode, rightLeafNode);
        }
        return true;
    }

    public LeafNode[] splitNode(MyPoint point){
        /*
        由于当前叶子容量满了，在这个节点里面加入一个点point会导致节点分裂
        分裂完成后，将新的叶子节点加到老的叶子节点的父亲中去
         */
        //根据点的类型，属性分成两组
        int[][] group = splitStrategy(point);
        LeafNode leftLeafNode = new LeafNode(getFather());
        LeafNode rightLeafNode = new LeafNode(getFather());

        for(int i = 0; i < group[0].length; i++){
            int pointIndex = group[0][i];
            leftLeafNode.insertPoint(pointArray[pointIndex]);
        }

        for(int i = 0; i < group[1].length; i++){
            int pointIndex = group[1][i];
            rightLeafNode.insertPoint(pointArray[pointIndex]);
        }

        return new LeafNode[] {leftLeafNode, rightLeafNode};
    }

    public int[][] splitStrategy(MyPoint point){
        /*
        定义叶子节点分裂策略：
        首先根据data[0]，即event type来分出两个叶子节点
        case 1: 某一类型的事件很多，其他类型的事件很少
        case 2: 全是一个类型，则按照属性值分类
        返回二维数组[2][?].
         */
        int maxNumOfType;

        return null;
    }

    @Override
    public MyPoint[] search(Rectangle rectangle){
        /*
        根据给定的范围，查找出满足条件的点
        比如给定的是:
        0 <= sun.price <= 2 AND 0 <= sun.volume <= 2
        Point的维度是3，分别是type in [0], price in [0, 2], volume in [0,2]
        rectangle leftBottom [0, 0, 0] rightTop [0, 2, 2]
        用vector保存满足条件的点
         */
        int count = 0;
        Vector<MyPoint> ans = new Vector<MyPoint>(16, 16);
        for(int i = 0; i < insertPos; i++){
            if(rectangle.containPoint(pointArray[i])){
                count++;
                ans.add((MyPoint) pointArray[i].clone());
            }
        }

        return null;
    }

    public static void main(String[] args){
        /*
        测试这个类方法是否写对
         */
    }
}

