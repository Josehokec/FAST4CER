package JoinStrategy;

import java.util.List;

/**
 * 处理RID的情况
 * @param timeList 时间戳list
 * @param matchList 匹配结果list
 */
record PartialBytesResultWithTime (List<Long> timeList, List<byte[]> matchList){ }
