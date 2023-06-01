package Method;

/**
 * 选择率
 * @param index         对应的独立谓词列表中的第几个
 * @param selectivity   predicate selectivity
 */
public record SelectivityPair(int index, double selectivity) { }
