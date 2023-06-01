package JoinStrategy;

import Common.EventPattern;

import java.util.List;

/**
 * 动态编程Join
 * page 1353
 */
public class DynamicProgrammingJoin extends AbstractJoinStrategy{
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
