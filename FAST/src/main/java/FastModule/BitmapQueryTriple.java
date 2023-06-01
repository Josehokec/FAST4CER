package FastModule;

import Store.RID;

/**
 * RID Timestamp VarID
 */
public record BitmapQueryTriple(int varId, long timestamp, RID rid) { }
