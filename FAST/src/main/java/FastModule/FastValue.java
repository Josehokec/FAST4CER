package FastModule;

import Method.IthRID;
import Store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FastValue {
    private List<RangeBitmap> rangeBitmaps;         // Range Bitmap Block
    private List<Long> timestamps;                  // TimeStamp Block
    private List<RID> rids;                         // RID Block

    public FastValue(int indexAttrNum, int recordNum) {
        rangeBitmaps = new ArrayList<>(indexAttrNum);
        timestamps = new ArrayList<>(recordNum);
        rids = new ArrayList<>(recordNum);
    }

    /**
     * Construct range bitmaps for a batch of data record
     * @param quads             FastTemporaryQuad list
     * @param indexAttrNum      number of index attribute
     * @param attrMaxRange      attribute max range
     */
    public void insertBatch(List<FastTemporaryTriple> quads, int indexAttrNum, List<Long> attrMaxRange){
        final int recordNum = quads.size();
        // 能有最大范围最好了，可以节省空间, 设置属性最大值与最小值
        RangeBitmap.Appender[] appends = new RangeBitmap.Appender[indexAttrNum];
        if(attrMaxRange != null){
            for(int i = 0; i < indexAttrNum; ++i){
                appends[i] = RangeBitmap.appender(attrMaxRange.get(i));
            }
        }else{
            for(int i = 0; i < indexAttrNum; ++i){
                appends[i] = RangeBitmap.appender(Long.MAX_VALUE >> 1);
            }
        }

        timestamps = new ArrayList<>(recordNum);
        rids = new ArrayList<>(recordNum);

        for(FastTemporaryTriple quad : quads){
            long[] attrValues = quad.attrValues();
            for(int i = 0; i < indexAttrNum; ++i){
                appends[i].add(attrValues[i]);
            }
            timestamps.add(quad.timestamp());
            rids.add(quad.rid());
        }

        // Construct range bitmap
        for(int i = 0; i < indexAttrNum; ++i){
            rangeBitmaps.add(appends[i].build());
        }
    }

    /**
     * perform a range query on the attribute columns of a certain index
     * @param idx   index attribute column number
     * @param min   query attribute minimum value
     * @param max   query attribute maximum value
     * @return      roaring bitmap that meet the conditions
     */
    public final RoaringBitmap betweenQuery(int idx, long min, long max){
        return rangeBitmaps.get(idx).between(min, max);
    }

    /**
     * perform a range query on the attribute columns of a certain index
     * @param idx   index attribute column number
     * @param max   query attribute maximum value
     * @return      roaring bitmap that meet the conditions
     */
    public final RoaringBitmap lteQuery(int idx, long max){
        return rangeBitmaps.get(idx).lte(max);
    }

    /**
     * perform a range query on the attribute columns of a certain index
     * @param idx   index attribute column number
     * @param min   query attribute minimum value
     * @return      roaring bitmap that meet the conditions
     */
    public final RoaringBitmap gteQuery(int idx, long min){
        return rangeBitmaps.get(idx).gte(min);
    }

    public final RoaringBitmap equalQuery(int idx, long value){
        return rangeBitmaps.get(idx).between(value, value);
    }

    /**
     * Directly returning a numbered RID list and then reading byte records
     * which is faster than reading byte records based on RIDs
     * @param rb        roaring bitmap
     * @param varId     which variable to query
     * @return          RID List with Ith
     */
    public final List<IthRID> getRIDList(RoaringBitmap rb, int varId){
        final int len = rb.getCardinality();
        List<IthRID> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i ->{
            RID rid = rids.get(i);
            ans.add(new IthRID(rid, varId));
        });
        return ans;
    }

    public final List<BitmapQueryTriple> getQueryTriples(RoaringBitmap rb, int varId){
        final int len = rb.getCardinality();
        List<BitmapQueryTriple> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i ->{
            long timestamp = timestamps.get(i);
            RID rid = rids.get(i);
            ans.add(new BitmapQueryTriple(varId, timestamp, rid));
        });

        return ans;
    }

    public final List<RidTimePair> getRidTimePairs(RoaringBitmap rb){
        final int len = rb.getCardinality();
        List<RidTimePair> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i ->{
            long timestamp = timestamps.get(i);
            RID rid = rids.get(i);
            ans.add(new RidTimePair(rid, timestamp));
        });

        return ans;
    }
}
