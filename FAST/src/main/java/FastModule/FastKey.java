package FastModule;

import java.util.Arrays;
import java.util.List;

/**
 * discard content
 *
 * key = typeId + dataPartitionId
 * to accelerate query, we store the attribute synopsis in FastKey
 * attribute synopsis contains startTime, endTime, minValues and maxValues
 */
public class FastKey {
    private final int[] key;
    private final long startTime;
    private final long endTime;
    private List<Long> minValues;
    private List<Long> maxValues;

    public FastKey(int typeId, int partitionId, long startTime, long endTime,
                   List<Long> minValues, List<Long> maxValues) {
        key = new int[2];
        key[1] = typeId;
        key[0] = partitionId;
        // attribute synopsis
        this.startTime = startTime;
        this.endTime = endTime;
        this.minValues = minValues;
        this.maxValues = maxValues;
    }

    public int[] getKey(){
        return key;
    }

    public int getTypeId() {
        return key[1];
    }

    public int getPartitionId() {
        return key[0];
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    /**
     * this value has been transformed
     * @param ith       position
     * @return          max value
     */
    public final long getIthIndexAttrMinValue(int ith){
        return minValues.get(ith);
    }

    /**
     * this value has been transformed
     * @param ith       position
     * @return          max value
     */
    public final long getIthIndexAttrMaxValue(int ith){
        return maxValues.get(ith);
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(key);
    }

    @Override
    public boolean equals(Object o){
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        FastKey k = (FastKey) o;
        return Arrays.equals(key, k.getKey());
    }
}
