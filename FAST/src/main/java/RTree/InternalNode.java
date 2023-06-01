package RTree;

import java.util.List;

public class InternalNode extends RTreeNode {
    /*
    内部节点需要存儿子节点的矩阵框，又要存指向儿子节点的指针
    儿子节点数量应该等于矩形数量
    矩形要排序
     */
    public List<RTreeNode> children;        // ptr
    public Rectangle[] rectangles;          // internal node data
    public int insertPos;                   // insert position
    // 是不是加个mbr会好一些？

    public InternalNode() {
        super(NodeType.INTERNAL_NODE);
        // 之所以要比最大条目数量多1，为了方便到时候分裂的时候不需要再传参数
        rectangles = new Rectangle[ArgsConfig.MAX_ENTRIES_IN_NODE + 1];
        // 是否需要把孩子节点给new一下？个人认为不要
        children = null;
        insertPos = 0;
    }

    public RTreeNode getChild(int index){
        // exception
        return children.get(index);
    }

    public void updateBoundary(){
        // 由于孩子节点插入了新的数据，节点新的边界需要更新
        // 如果新加入的元素没有引起矩阵边框扩大，那么提前结束边界更新
        // 对应adjust tree
    }

    public void updateEntries(RTreeNode node1, RTreeNode node2){
        //儿子节点分裂成node1和node2了
        // 对应adjust tree
    }

    public void insertRectangle(Rectangle rectangle){
        if(rectangle == null){
            throw new IllegalArgumentException("cannot insert null rectangle");
        }else if(insertPos == ArgsConfig.MAX_ENTRIES_IN_NODE){
            throw new IllegalArgumentException("Internal Node has full");
        }else{
            //
            rectangles[insertPos++] = rectangle;
        }
    }

    public boolean insertChild(RTreeNode node){
        // 在孩子节点数组加入，然后更新对应的矩形边界
        return true;
    }

    // if insertPos == ArgsConfig.MAX_ENTRIES_IN_NODE
    // then use this function insert Rectangle
    public int[][] quadraticSplit(Rectangle rectangle){
        /*
        触发内部节点分裂的函数，比较难实现
         */


        return null;
    }

    public static void main(String[] args){
        //测试这个类的方法是否正确
    }

}
