package BPlusTree;

import Common.IndexValuePair;

import java.util.ArrayList;
import java.util.List;

/**
 * 这个版本存在bug
 * 实现B+树，参考了<a href="https://blog.csdn.net/qq_33171970/article/details/88395278">BPlusTreeCode</a>
 * T是值 V是键
 * @param <T> 指定值类型
 * @param <V> 指定索引类型键,并且指定必须继承Comparable
 * B+树保留的参数     含义
 * bTreeOrder       b+树的阶
 * minNUmber        B+树的非叶子节点最小拥有的子节点数量（已经被废弃）构造的时候能保证这一点
 * maxNumber        B+树的非叶子节点最大拥有的节点数量（同时也是键的最大数量）
 * maxValue         记录最大值
 * minValue         记录最小值
 * root             B+树的根节点
 * left             B+树的最右边的叶子节点
 */
public class MyBPlusTree<T, V extends Comparable<V>>{
    private final int bTreeOrder;
    private final int maxNumber;
    private V maxValue;
    private V minValue;
    private Node<T, V> root;
    // 叶子节点的最左边节点
    private LeafNode<T, V> left;

    /**
     * 插入元组到节点过程中，是先插入再判断是否要分裂
     * 因此可能出现超过上限的情况，所以这里要加1
     * @param bTreeOrder b+树的阶
     */
    public MyBPlusTree(Integer bTreeOrder){
        this.bTreeOrder = bTreeOrder;
        this.maxNumber = bTreeOrder + 1;
        this.root = new LeafNode<>();
        this.left = null;
        maxValue = null;
        minValue =null;
    }

    /**
     * 给定key，找到key对应的value
     * @param key 要查找的key
     * @return 查到的value，如果查不到返回null
     */
    public T find(V key){
        return this.root.find(key);
    }

    /**
     * 给定key找到大于等于key的最小key所在的叶子节点
     * @param key 要找的最小值
     * @return 最小值所在的叶子节点
     */
    public LeafNode<T,V> findLeafUsingKey(V key){
        return this.root.findLeafUsingKey(key);
    }

    /**
     * 范围查询（等待实现）
     * @param min 范围查询最小值
     * @param max 范围查询最大值
     * @return 满足条件的值列表
     */
    public List<T> rangeQuery(V min, V max){
        LeafNode<T, V> startNode;
        List<T> ans = new ArrayList<>();

        startNode = (min.compareTo(minValue) <= 0) ? left : findLeafUsingKey(min);

        V startKey = (V) startNode.keys[0];
        while(startKey.compareTo(max) <= 0){
            for(int i = 0; i < startNode.number; ++i){
                V curKey = (V) startNode.keys[i];
                if(curKey.compareTo(max) <= 0 && curKey.compareTo(min) >= 0){
                    ans.add((T) startNode.values[i]);
                }
            }
            // 如果还有右相邻节点
            if(startNode.right != null){
                startNode = startNode.right;
                startKey = (V) startNode.keys[0];
            }else{
                break;
            }
        }
        return ans;
    }

    /**
     * 访问叶子节点所有的值
     * @return 所有的叶子节点的值列表
     */
    public List<T> getAllValues(){
        List<T> ans = new ArrayList<>();
        LeafNode<T, V> next = left;
        while(next != null){
            for(int i = 0; i < next.number; ++i){
                ans.add((T) next.values[i]);
            }
            next = next.right;
        }
        return ans;
    }

    public List<V> getAllKeys(){
        List<V> ans = new ArrayList<>();
        LeafNode<T, V> next = left;
        while(next != null){
            for(int i = 0; i < next.number; ++i){
                ans.add((V) next.keys[i]);
            }
            next = next.right;
        }
        return ans;
    }

    /**
     * 插入键值对，如果键是空则直接返回
     * @param value 值
     * @param key 键
     */
    public void insert(T value, V key){
        if(key == null){
            return;
        }
        if(maxValue == null){
            maxValue = key;
            minValue = key;
        }else{
            if(key.compareTo(maxValue) > 0){
                maxValue = key;
            }
            if(key.compareTo(minValue) < 0){
                minValue = key;
            }
        }
        Node<T, V> t = this.root.insert(value, key);
        if(t != null){
            this.root = t;
        }
        // 更新最左边的值
        this.left = this.root.refreshLeft();
    }

    /**
     * 节点父类，因为在B+树中，非叶子节点不用存储具体的数据，只需要把索引作为键就可以了
     * 所以叶子节点和非叶子节点的类不太一样，但是又会公用一些方法，所以用Node类作为父类,
     * 而且因为要互相调用一些公有方法，所以使用抽象类
     * @param <T> 同BPlusTree
     * @param <V> 键
     * parent 父节点
     * children 孩子节点
     * number 子节点数量
     * keys 保存的key数量
     */
    abstract class Node<T, V extends Comparable<V>>{
        protected Node<T, V> parent;
        protected Node<T, V>[] children;
        protected Integer number;
        protected Object[] keys;
        public Node(){
            this.keys = new Object[maxNumber];
            this.children = new Node[maxNumber];
            this.number = 0;
            this.parent = null;
        }

        /**
         * 根据key找到对应的value适用于等值查询
         */
        abstract T find(V key);

        /**
         * 找到大于等于key的最小key所在的叶子节点
         * @param key 要查找范围的最小值
         * @return 所在的叶子节点
         */
        abstract LeafNode<T,V> findLeafUsingKey(V key);
        //插入
        abstract Node<T, V> insert(T value, V key);
        abstract LeafNode<T, V> refreshLeft();
    }

    /**
     * 内部节点，即非叶节点类，root也是非叶子节点
     * @param <T> 值
     * @param <V> 键
     */
    class InternalNode <T, V extends Comparable<V>> extends Node<T, V>{
        public InternalNode() {
            super();
        }

        /**
         * 递归查找,这里只是为了确定值究竟在哪一块,真正的查找到叶子节点才会查
         * 这里使用二分查找来加速，原版使用的是顺序查找
         * @param key 要查的key
         * @return 查到的value
         */
        @Override
        T find(V key) {
            // 二分查找会快一些 key如果小于非叶子节点的key的大小
            int start = 0;
            int end = this.number - 1;
            // 如果比最大值还大，说明找不到了
            if(key.compareTo((V) this.keys[end]) > 0){
                return null;
            }else{
                int mid = (start + end) >> 1;
                // 正常应该是start <= end的，但是这里不能等
                while(start < end){
                    if(key.compareTo((V) this.keys[mid]) > 0){
                        start = mid + 1;
                    }else{
                        end = mid;
                    }
                    mid = (start + end) >> 1;
                }
                return this.children[mid].find(key);
            }
        }

        /**
         * 找到大于等于key的最小key所在的叶子节点
         * @param key 范围查询的最小值
         * @return 叶子节点
         */
        @Override
        LeafNode<T,V> findLeafUsingKey(V key){
            int start = 0;
            int end = this.number - 1;
            // 如果比最大值还大，说明找不到了
            if(key.compareTo((V) this.keys[end]) > 0){
                return null;
            }else{
                int mid = (start + end) >> 1;
                while(start < end){
                    if(key.compareTo((V) this.keys[mid]) > 0){
                        start = mid + 1;
                    }else{
                        end = mid;
                    }
                    mid = (start + end) >> 1;
                }
                return this.children[mid].findLeafUsingKey(key);
            }
        }

        /**
         * 递归插入,先把值插入到对应的叶子节点,最终讲调用叶子节点的插入类
         * @param value
         * @param key
         */
        @Override
        Node<T, V> insert(T value, V key) {
            int i = 0;
            while(i < this.number){
                if(key.compareTo((V) this.keys[i]) < 0)
                    break;
                i++;
            }
            if(key.compareTo((V) this.keys[this.number - 1]) >= 0) {
                i--;
            }
            return this.children[i].insert(value, key);
        }

        @Override
        LeafNode<T, V> refreshLeft() {
            return this.children[0].refreshLeft();
        }

        /**
         * 当叶子节点插入成功完成分解时,递归地向父节点插入新的节点以保持平衡
         * @param node1 分类出来的新的节点1
         * @param node2 分裂出来的新的节点2
         * @param key 新分裂节点的最大值
         */
        Node<T, V> insertNode(Node<T, V> node1, Node<T, V> node2, V key){
            //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1]);
            V oldKey = null;
            if(this.number > 0)
                oldKey = (V) this.keys[this.number - 1];
            //如果原有key为null,说明这个非节点是空的,直接放入两个节点即可
            if(key == null || this.number <= 0){
                //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + "直接插入");
                this.keys[0] = node1.keys[node1.number - 1];
                this.keys[1] = node2.keys[node2.number - 1];
                this.children[0] = node1;
                this.children[1] = node2;
                this.number += 2;
                return this;
            }
            //原有节点不为空,则应该先寻找原有节点的位置,然后将新的节点插入到原有节点中
            int i = 0;
            while(key.compareTo((V)this.keys[i]) != 0){
                i++;
            }
            //左边节点的最大值可以直接插入,右边的要挪一挪再进行插入
            this.keys[i] = node1.keys[node1.number - 1];
            this.children[i] = node1;

            Object[] tempKeys = new Object[maxNumber];
            Object[] tempChildren = new Node[maxNumber];

            System.arraycopy(this.keys, 0, tempKeys, 0, i + 1);
            System.arraycopy(this.children, 0, tempChildren, 0, i + 1);
            System.arraycopy(this.keys, i + 1, tempKeys, i + 2, this.number - i - 1);
            System.arraycopy(this.children, i + 1, tempChildren, i + 2, this.number - i - 1);
            tempKeys[i + 1] = node2.keys[node2.number - 1];
            tempChildren[i + 1] = node2;

            this.number++;

            //判断是否需要拆分
            //如果不需要拆分,把数组复制回去,直接返回
            if(this.number <= bTreeOrder){
                System.arraycopy(tempKeys, 0, this.keys, 0, this.number);
                System.arraycopy(tempChildren, 0, this.children, 0, this.number);
                //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ", 不需要拆分");
                return null;
            }

            //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ",需要拆分");
            //如果需要拆分,和拆叶子节点时类似,从中间拆开
            Integer middle = this.number / 2;

            //新建非叶子节点,作为拆分的右半部分
            InternalNode<T, V> tempNode = new InternalNode<T, V>();
            //非叶节点拆分后应该将其子节点的父节点指针更新为正确的指针
            tempNode.number = this.number - middle;
            tempNode.parent = this.parent;
            //如果父节点为空,则新建一个非叶子节点作为父节点,并且让拆分成功的两个非叶子节点的指针指向父节点
            if(this.parent == null) {

                //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ",新建父节点");
                InternalNode<T, V> tempInternalNode = new InternalNode<>();
                tempNode.parent = tempInternalNode;
                this.parent = tempInternalNode;
                oldKey = null;
            }
            System.arraycopy(tempKeys, middle, tempNode.keys, 0, tempNode.number);
            System.arraycopy(tempChildren, middle, tempNode.children, 0, tempNode.number);
            for(int j = 0; j < tempNode.number; j++){
                tempNode.children[j].parent = tempNode;
            }
            //让原有非叶子节点作为左边节点
            this.number = middle;
            this.keys = new Object[maxNumber];
            this.children = new Node[maxNumber];
            System.arraycopy(tempKeys, 0, this.keys, 0, middle);
            System.arraycopy(tempChildren, 0, this.children, 0, middle);

            //叶子节点拆分成功后,需要把新生成的节点插入父节点
            InternalNode<T, V> parentNode = (InternalNode<T, V>)this.parent;
            return parentNode.insertNode(this, tempNode, oldKey);
        }

    }

    /**
     * 叶节点类
     * @param <T> value的数据类型
     * @param <V> key的数据类型
     */
    class LeafNode <T, V extends Comparable<V>> extends Node<T, V> {

        protected Object values[];
        protected LeafNode left;
        protected LeafNode right;

        public LeafNode(){
            super();
            this.values = new Object[maxNumber];
            this.left = null;
            this.right = null;
        }

        /**
         * 原始版本的二分查找存在bug，这里进行了修复
         * @param key 要查找的key
         * @return 这个key对应的值
         */
        @Override
        T find(V key) {
            if(this.number <= 0){
                return null;
            }
            int left = 0;
            int right = this.number;

            while(left <= right){
                int mid = (left + right) >> 1;
                if(key.compareTo((V) this.keys[mid]) == 0){
                    return (T) this.values[mid];
                }else if(key.compareTo((V) this.keys[mid]) < 0){
                    right = mid - 1;
                }else{
                    left = mid + 1;
                }
            }
            return null;
        }

        /**
         * @param key 要查找范围的最小值
         * @return 叶子节点
         */
        @Override
        LeafNode<T,V> findLeafUsingKey(V key){
            if(this.number <=0){
                return null;
            }else{
                return this;
            }
        }

        /**
         * 在叶子节点插入键值对
         * @param value 值
         * @param key 键
         */
        @Override
        Node<T, V> insert(T value, V key) {
            //System.out.println("叶子节点,插入key: " + key);

            //保存原始存在父节点的key值
            V oldKey = null;
            if(this.number > 0)
                oldKey = (V) this.keys[this.number - 1];
            //先插入数据，要找到插入的位置
            int i = 0;
            while(i < this.number){
                if(key.compareTo((V) this.keys[i]) < 0)
                    break;
                i++;
            }

            //复制数组,完成添加
            Object[] tempKeys = new Object[maxNumber];
            Object[] tempValues = new Object[maxNumber];
            //[0,0+i)
            System.arraycopy(this.keys, 0, tempKeys, 0, i);
            System.arraycopy(this.values, 0, tempValues, 0, i);
            System.arraycopy(this.keys, i, tempKeys, i + 1, this.number - i);
            System.arraycopy(this.values, i, tempValues, i + 1, this.number - i);
            tempKeys[i] = key;
            tempValues[i] = value;

            this.number++;

            //System.out.println("插入完成,当前节点key为:");
            //for(int j = 0; j < this.number; j++)
            //  System.out.print(tempKeys[j] + " ");
            //System.out.println();

            //判断是否需要拆分
            //如果不需要拆分完成复制后直接返回
            if(this.number <= bTreeOrder){
                System.arraycopy(tempKeys, 0, this.keys, 0, this.number);
                System.arraycopy(tempValues, 0, this.values, 0, this.number);

                //有可能虽然没有节点分裂，但是实际上插入的值大于了原来的最大值，所以所有父节点的边界值都要进行更新
                Node node = this;
                while (node.parent != null){
                    V tempkey = (V)node.keys[node.number - 1];
                    if(tempkey.compareTo((V)node.parent.keys[node.parent.number - 1]) > 0){
                        node.parent.keys[node.parent.number - 1] = tempkey;
                        node = node.parent;
                    }
                    else {
                        break;
                    }
                }
                //System.out.println("叶子节点,插入key: " + key + ",不需要拆分");
                return null;
            }

            //System.out.println("叶子节点,插入key: " + key + ",需要拆分");
            //如果需要拆分,则从中间把节点拆分差不多的两部分
            Integer middle = this.number / 2;

            //新建叶子节点,作为拆分的右半部分
            LeafNode<T, V> tempNode = new LeafNode<T, V>();
            tempNode.number = this.number - middle;
            tempNode.parent = this.parent;
            //如果父节点为空,则新建一个非叶子节点作为父节点,并且让拆分成功的两个叶子节点的指针指向父节点
            if(this.parent == null) {
                //System.out.println("叶子节点,插入key: " + key + ",父节点为空 新建父节点");
                InternalNode<T, V> tempInternalNode = new InternalNode<>();
                tempNode.parent = tempInternalNode;
                this.parent = tempInternalNode;
                oldKey = null;
            }
            System.arraycopy(tempKeys, middle, tempNode.keys, 0, tempNode.number);
            System.arraycopy(tempValues, middle, tempNode.values, 0, tempNode.number);

            //让原有叶子节点作为拆分的左半部分
            this.number = middle;
            this.keys = new Object[maxNumber];
            this.values = new Object[maxNumber];
            System.arraycopy(tempKeys, 0, this.keys, 0, middle);
            System.arraycopy(tempValues, 0, this.values, 0, middle);

            this.right = tempNode;
            tempNode.left = this;

            //叶子节点拆分成功后,需要把新生成的节点插入父节点
            InternalNode<T, V> parentNode = (InternalNode<T, V>)this.parent;
            return parentNode.insertNode(this, tempNode, oldKey);
        }

        @Override
        LeafNode<T, V> refreshLeft() {
            if(this.number <= 0)
                return null;
            return this;
        }
    }
}

/*
原始内部节点的find函数
int i = 0;
while(i < this.number){
    if(key.compareTo((V) this.keys[i]) <= 0)
        break;
    i++;
}
if(this.number == i)
    return null;
return this.children[i].find(key);

原始叶子节点的find函数
if(this.number <=0){
    return null;
}
//System.out.println("叶子节点查找");
Integer left = 0;
Integer right = this.number;
Integer middle = (left + right) / 2;

while(left < right){
    V middleKey = (V) this.keys[middle];
    if(key.compareTo(middleKey) == 0)
        return (T) this.values[middle];
    else if(key.compareTo(middleKey) < 0)
        right = middle;
    else
        left = middle;
    middle = (left + right) / 2;
}
return null;
 */
