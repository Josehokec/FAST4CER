package SkipList;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ref：<a href="https://www.freesion.com/article/4513366377/">SkipList</a>
 * */
public class SkipList <T>{
    private int nodes;                                  // node number
    private int listLevel;                              // level
    private SkipListNode<T> head,tail;                  // head or tail pointer
    private final Random random;                        // random
    private static final double PROBABILITY = 0.42;     //prob
    public SkipList() {
        random=new Random();
        initial();
    }

    /**
     * initial skip list
     **/
    public void initial(){
        head=new SkipListNode<T>(SkipListNode.HEAD_KEY, null);
        tail=new SkipListNode<T>(SkipListNode.TAIL_KEY, null);
        horizontalLink(head, tail);
        listLevel=0;
        nodes=0;
    }
    public boolean isEmpty(){
        return nodes == 0;
    }

    public int size() {
        return nodes;
    }

    /**
     * on the bottom layer, locate the key in front of the position you want to insert
     * */
    private SkipListNode<T> findNode(long key){
        SkipListNode<T> p=head;
        while(true){
            while (p.right.key != SkipListNode.TAIL_KEY && p.right.key <= key) {
                p = p.right;
            }
            if(p.down != null) {
                p = p.down;
            }else {
                break;
            }

        }
        return p;
    }

    /**
     * This function is applicable to equivalence queries
     * Find if the key is stored, return the node if it exists, otherwise return null
     * */
    public SkipListNode<T> search(long key){
        SkipListNode<T> p=findNode(key);
        if (key==p.getKey()) {
            return p;
        }else {
            return null;
        }
    }

    /**
     * rangeQuery
     * @param min minimum
     * @param max maximum
     * @return value list
     */
    public List<T> rangeQuery(long min, long max){
        List<T> ans = new ArrayList<>();
        // Start node, note that the value of the start node is less than min
        SkipListNode<T> node;
        if(min == Long.MIN_VALUE){
            node= findNode(min);
        }else{
            node= findNode(min - 1);
        }

        while(node != null && node.getKey() <= max){
            if(node.getValue() != null && node.getKey() >= min){
                ans.add(node.getValue());
            }
            node = node.right;
        }
        return ans;
    }

    public void insert(long k, T v){
        put(k, v);
    }

    /**
     * add key-value
     * we can insert duplicate tuples
     * */
    public void put(long k,T v){
        SkipListNode<T> p=findNode(k);

        SkipListNode<T> q=new SkipListNode<T>(k, v);
        backLink(p, q);
        int currentLevel = 0;

        while (random.nextDouble() < PROBABILITY) {

            if (currentLevel>=listLevel) {
                listLevel++;
                SkipListNode<T> p1=new SkipListNode<T>(SkipListNode.HEAD_KEY, null);
                SkipListNode<T> p2=new SkipListNode<T>(SkipListNode.TAIL_KEY, null);
                horizontalLink(p1, p2);
                verticalLink(p1, head);
                verticalLink(p2, tail);
                head=p1;
                tail=p2;
            }

            while (p.up == null) {
                p = p.left;
            }
            p = p.up;

            SkipListNode<T> e=new SkipListNode<T>(k, null);
            backLink(p, e);
            verticalLink(e, q);
            q=e;
            currentLevel++;
        }
        nodes++;
    }

    private void backLink(SkipListNode<T> node1,SkipListNode<T> node2){
        node2.left=node1;
        node2.right=node1.right;
        node1.right.left=node2;
        node1.right=node2;
    }


    private void horizontalLink(SkipListNode<T> node1,SkipListNode<T> node2){
        node1.right=node2;
        node2.left=node1;
    }


    private void verticalLink(SkipListNode<T> node1, SkipListNode<T> node2){
        node1.down=node2;
        node2.up=node1;
    }

    /**
     * print SkipList
     * */
    @Override
    public String toString() {
        if (isEmpty()) {
            return "Skip List is null";
        }
        StringBuilder builder=new StringBuilder();
        SkipListNode<T> p = head;
        while (p.down!=null) {
            p=p.down;
        }

        while (p.left!=null) {
            p=p.left;
        }
        if (p.right!=null) {
            p=p.right;
        }
        while (p.right!=null) {
            builder.append(p);
            builder.append("\n");
            p=p.right;
        }

        return builder.toString();
    }

}
