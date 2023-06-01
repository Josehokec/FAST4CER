package RTree;

import Common.IndexValuePair;
import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;


import java.util.ArrayList;
import java.util.List;

/**
 * 应该要设置权重的
 * 开源的RTree项目Point全是浮点数<br>
 * 但是我们定义的是整型，因此在这里我们需要转换成double类型的
 */
public class MemoryRTree implements RTreeInterface {
    RTree<IndexValuePair, Point> rtree;

    public MemoryRTree(int dimension){
        // 目前测试了一下 孩子节点在16时候插入和删除性能比较好
        rtree = RTree.dimensions(dimension).maxChildren(16).create();
    }
    @Override
    public void insert(long[] key, IndexValuePair value) {
        double[] x = new double[key.length];
        for(int i = 0; i < key.length; ++i){
            x[i] = key[i];
        }
        rtree = rtree.add(value, Point.create(x));
    }

    @Override
    public List<IndexValuePair> rangeQuery(long[] min, long[] max) {
        assert min.length == max.length : "parameters have error.";
        List<IndexValuePair> list = new ArrayList<>();

        double[] leftDown = new double[min.length];
        double[] rightUp = new double[min.length];
        for(int i = 0; i < min.length; ++i){
            if(min[i] == Long.MIN_VALUE){
                leftDown[i] = Double.MIN_VALUE;
            }else{
                leftDown[i] = min[i];
            }

            if(max[i] == Long.MAX_VALUE){
                rightUp[i] = Double.MAX_VALUE;
            }else{
                rightUp[i] = max[i];
            }

            Iterable<Entry<IndexValuePair, Point>> entries =
                    rtree.search(Rectangle.create(leftDown, rightUp));
            //使用lambda表达式取value列表
            entries.forEach(pair->{
                list.add(pair.value());
            });
        }

        return list;
    }
}
