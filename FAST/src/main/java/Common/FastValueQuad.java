package Common;

import Store.RID;

public class FastValueQuad {
    private long[] attrValues;
    private byte[] tsRight;
    private byte[] bytesRecord;
    private RID rid;
    public FastValueQuad(long[] attrValues, byte[] tsRight, RID rid, byte[] bytesRecord) {
        this.attrValues = attrValues;
        this.tsRight = tsRight;
        this.rid = rid;
        this.bytesRecord = bytesRecord;
    }

    public long[] getAttrValues() {
        return attrValues;
    }

    public byte[] getTsRight() {
        return tsRight;
    }

    public RID getRid() {
        return rid;
    }

    public byte[] getBytesRecord(){
        return bytesRecord;
    }
}
