package RTree;

// R tree has three types node: Root, Internal Node and LeafNode
enum NodeType {ROOT, INTERNAL_NODE, LEAF_NODE}

public abstract class RTreeNode {
    /*
    RTreeNode has three types: root, internal node and leaf node
    internal node store rectangle, leaf node store point
    when split, need to know node father

    rTree ptr and level vital ?
     */
    public int level;                  //node level
    public NodeType nodeType;          // node type
    public RTreeNode father;           //node father


    public RTreeNode(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public RTreeNode(NodeType nodeType, RTreeNode father, int level) {
        this.nodeType = nodeType;
        this.father = father;
        this.level = level;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public RTreeNode getFather(){
        return father;
    }

    public MyPoint[] search(Rectangle rectangle){
        /*
        根据给定的范围，查找出满足条件的点
        比如给定的是:
        0 <= sun.price <= 2 AND 0 <= sun.volume <= 2
        Point的维度是3，分别是type in [0], price in [0, 2], volume in [0,2]
        rectangle leftBottom [0, 0, 0] rightTop [0, 2, 2]
        用队列来求，问题：满足条件的Point数量可能很多

         */
        return null;
    }

    public boolean isRoot(){
        return nodeType == NodeType.ROOT;
    }

    public boolean isInternalNode(){
        return nodeType == NodeType.INTERNAL_NODE;
    }

    public boolean isLeafNode(){
        return nodeType == NodeType.LEAF_NODE;
    }

}
