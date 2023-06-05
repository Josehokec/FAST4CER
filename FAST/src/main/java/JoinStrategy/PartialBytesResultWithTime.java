package JoinStrategy;

import java.util.List;

/**
 * Join partial match results
 * @param timeList timestamp list
 * @param matchList matched result list
 */
record PartialBytesResultWithTime (List<Long> timeList, List<byte[]> matchList){ }
