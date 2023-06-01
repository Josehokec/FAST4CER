package Common;

import Store.RID;

/**
 * 时间戳
 * 记录存储的位置
 */
public class FastValuePair {
    private long timestamp;
    private RID rid;

    public FastValuePair(long timestamp, RID rid) {
        this.timestamp = timestamp;
        this.rid = rid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public RID getRid() {
        return rid;
    }
}
