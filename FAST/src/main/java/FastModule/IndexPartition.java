package FastModule;

import Condition.IndependentConstraintQuad;
import Store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class IndexPartition {
    private HashMap<String, SegmentInfo> infoMap;   // synopsis information
    private List<RangeBitmap> rangeBitmapList;      // range bitmap list
    private List<Long> timestamps;                  // timestamp list
    private List<RID> rids;                         // Row ID list

    public final void setInfoMap(HashMap<String, SegmentInfo> infoMap) {
        this.infoMap = infoMap;
    }

    public final void setRangeBitmapList(List<RangeBitmap> rangeBitmapList) {
        this.rangeBitmapList = rangeBitmapList;
    }

    public final void setTimestamps(List<Long> timestamps) {
        this.timestamps = timestamps;
    }

    public final void setRids(List<RID> rids) {
        this.rids = rids;
    }

    public final Set<String> getInfoMapKeySet(){
        return infoMap.keySet();
    }

    /**
     * Filter related record based on corresponding independent predicate constraints
     * @param eventType     event type
     * @param icQuads       independent predicate constraints
     * @return              <rid, timestamp> pair
     */
    public final List<RidTimePair> query(String eventType, List<IndependentConstraintQuad> icQuads){
        List<RidTimePair> pairs = new ArrayList<>();
        // info must be not null
        SegmentInfo info = infoMap.get(eventType);

        if(info == null){
            throw new RuntimeException("program has bug");
        }

        List<Long> minValues = info.minValues();
        List<Long> maxValues = info.maxValues();
        // step 1: compare min and max value, if in this range then need to range query, else no early break
        boolean earlyBreak = false;
        for(IndependentConstraintQuad quad : icQuads){
            int idx = quad.idx();
            long min = quad.min();
            long max = quad.max();
            if(maxValues.get(idx) < min || minValues.get(idx) > max){
               earlyBreak = true;
               break;
            }
        }

        // step 2: range query
        if(!earlyBreak){
            // finalRoaringBitmap is not null
            RoaringBitmap finalRoaringBitmap = getEventTypeBitmap(eventType);
            // assuming that all attributes of the query have indexes
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                int mark = quad.mark();
                long min = quad.min();
                long max = quad.max();

                RoaringBitmap rangeQueryRB = null;
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                switch (mark) {
                    case 1 -> rangeQueryRB = gteQuery(idx, min);
                    case 2 -> rangeQueryRB = lteQuery(idx, max);
                    case 3 -> rangeQueryRB = betweenQuery(idx, min, max);
                }

                if(rangeQueryRB == null){
                    finalRoaringBitmap = null;
                    break;
                }else{
                    finalRoaringBitmap.and(rangeQueryRB);
                }
            }

            if (finalRoaringBitmap != null) {
                // use bitmap to get <rid, timestamp> pair
                List<RidTimePair> ridTimePairs = getRidTimePairs(finalRoaringBitmap);
                pairs.addAll(ridTimePairs);
            }
        }
        return pairs;
    }

    /**
     * according to event type to generate roaring bitmap
     * @param eventType     event type
     * @return              roaring bitmap indicate position
     */
    public RoaringBitmap getEventTypeBitmap(String eventType){
        SegmentInfo info = infoMap.get(eventType);
        RoaringBitmap rb = new RoaringBitmap();
        int start = info.startPos();
        int offset = info.offset();
        rb.add(start,start + offset);
        return rb;
    }

    public RoaringBitmap gteQuery(int idx, long min){
        return rangeBitmapList.get(idx).gte(min);
    }

    public RoaringBitmap lteQuery(int idx, long max){
        return rangeBitmapList.get(idx).lte(max);
    }

    public RoaringBitmap betweenQuery(int idx, long min, long max){
        return rangeBitmapList.get(idx).between(min, max);
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
