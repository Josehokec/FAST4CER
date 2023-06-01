package FastModule;

import java.util.List;

public record SegmentInfo(int startPos, int offset,
                          long startTime, long endTime,
                          List<Long> minValues,
                          List<Long> maxValues) {

}
