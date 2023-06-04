package Common;

import Store.RID;

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
