package JoinStrategy;

import Common.EventPattern;

import java.util.List;

public abstract class AbstractJoinStrategy {

    /**
     * WithoutDC -> Without Dependent Constraint<br>
     * If there is no dependency on the predicate,
     * simply pass the timestamp directly to reduce the transmission event overhead<br>
     * S2 = skip-till-next-match<br>
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores timestamps
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countUsingS2WithoutDC(EventPattern pattern, List<List<Long>> buckets);

    /**
     * The string type must be passed here to handle S2<br>
     * WithoutDC -> Without Dependent Constraint<br>
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores string records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countUsingS2WithDC(EventPattern pattern, List<List<String>> buckets);

    /**
     * WithoutDC -> Without Dependent Constraint<br>
     * record is string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores string records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingS2WithRecord(EventPattern pattern, List<List<String>> buckets);

    /**
     * S2 = skip-till-next-match<br>
     * record is byte array
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * S2 = skip-till-next-match<br>
     * record is byte array
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * S3 = skip-till-any-match
     * record is string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores string records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countUsingS3WithDC(EventPattern pattern, List<List<String>> buckets);

    /**
     * S3 = skip-till-any-match
     * record is string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores string records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingS3WithRecord(EventPattern pattern, List<List<String>> buckets);

    /**
     * S3 = skip-till-any-match
     * record is byte array
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * S3 = skip-till-any-match
     * record is byte array
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets);
}
