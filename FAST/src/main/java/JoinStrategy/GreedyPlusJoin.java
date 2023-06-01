package JoinStrategy;

import Common.EventPattern;

import java.util.List;

/**
 * 贪心算法，每一次选择数量最小的事件
 * 假设我们知道了
 * arrival_i and selectivity_i_j
 * 算法具描述请见论文：
 * Efficient adaptive detection of complex event patterns
 * page1353
 */
public class GreedyPlusJoin extends AbstractJoinStrategy{
    @Override
    public int countUsingS2WithoutDC(EventPattern pattern, List<List<Long>> buckets) {
        return 0;
    }

    @Override
    public int countUsingS2WithDC(EventPattern pattern, List<List<String>> buckets) {
        return 0;
    }

    @Override
    public List<Tuple> getTupleUsingS2WithRecord(EventPattern pattern, List<List<String>> buckets) {
        return null;
    }

    @Override
    public int countUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        return 0;
    }

    @Override
    public List<Tuple> getTupleUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        return null;
    }

    @Override
    public int countUsingS3WithDC(EventPattern pattern, List<List<String>> buckets) {
        return 0;
    }

    @Override
    public List<Tuple> getTupleUsingS3WithRecord(EventPattern pattern, List<List<String>> buckets) {
        return null;
    }

    @Override
    public int countUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        return 0;
    }

    @Override
    public List<Tuple> getTupleUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets) {
        return null;
    }
}
