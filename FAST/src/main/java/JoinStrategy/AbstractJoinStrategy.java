package JoinStrategy;

import Common.EventPattern;

import java.util.List;

public abstract class AbstractJoinStrategy {

    /**
     * WithoutDC是Without Dependent Constraint缩写，表示没有依赖谓词约束<br>
     * 如果没有依赖谓词直接传时间戳就行了 减少传输事件开销<br>
     * S2 = skip-till-next-match策略<br>
     * @param pattern   查询的模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足元组的数量
     */
    public abstract int countUsingS2WithoutDC(EventPattern pattern, List<List<Long>> buckets);

    /**
     * 这里必须传字符串类型才能处理S2<br>
     * WithDC是With Dependent Constraint缩写，表示有依赖谓词约束<br>
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足元组的数量
     */
    public abstract int countUsingS2WithDC(EventPattern pattern, List<List<String>> buckets);

    /**
     * WithDC是With Dependent Constraint缩写，表示有依赖谓词约束<br>
     * 这里必须传字符串类型的才能处理了
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足元组的数量
     */
    public abstract List<Tuple> getTupleUsingS2WithRecord(EventPattern pattern, List<List<String>> buckets);

    /**
     * 匹配策略是skip-till-next-match<br>
     * 传过来是字节数组类型的记录
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足元组的数量
     */
    public abstract int countUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * 匹配策略是S2 skip till next match 传过来的是字节数组
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足条件的元组
     */
    public abstract List<Tuple> getTupleUsingS2WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * 匹配策略是S3 skip-till-any-match 传过来的是字符串
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足条件的元组数量
     */
    public abstract int countUsingS3WithDC(EventPattern pattern, List<List<String>> buckets);

    /**
     * 匹配策略是S3 skip-till-any-match 传过来的是字符串
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          返回元组
     */
    public abstract List<Tuple> getTupleUsingS3WithRecord(EventPattern pattern, List<List<String>> buckets);

    /**
     * 匹配策略是S3 skip-till-any-match 传过来的是字节数组
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足条件的元组数量
     */
    public abstract int countUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * 匹配策略是S3 skip-till-any-match 传过来的是字节数组
     * @param pattern   查询模式
     * @param buckets   满足独立谓词的各个变量对应的事件
     * @return          满足条件的元组
     */
    public abstract List<Tuple> getTupleUsingS3WithBytes(EventPattern pattern, List<List<byte[]>> buckets);
}
