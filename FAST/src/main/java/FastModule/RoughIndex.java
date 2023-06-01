package FastModule;

import Condition.IndependentConstraintQuad;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Due to the large amount of data stored in the buffer
 * when the selection rate is low, the cost of full scanning this part of the data is relatively high
 * Therefore, a rough index is constructed to filter irrelevant record
 * default a attribute has 5 roaring bitmaps, and it has 9 split point: split0, ..., split8
 */
public class RoughIndex {
    public static int splitNum = 10;             // each attribute is allocate 10 roaring bitmap to index data
    private int indexNum;                       // number of index attribute
    private List<Long> minValues;               // each attribute minimum value
    private List<Long> maxValues;               // each attribute maximum value
    private List<Long[]> attrSplitPoint;        // attrRanges.get(i).size = splitNum - 1
    private List<RoaringBitmap[]> indexes;      // for each index attribute, build rough index

    public RoughIndex(int indexNum, List<Long[]> splitPoints){
        this.indexNum = indexNum;
        minValues = new ArrayList<>(indexNum);
        maxValues = new ArrayList<>(indexNum);
        attrSplitPoint = new ArrayList<>(indexNum);
        indexes = new ArrayList<>(indexNum);
        for(int i = 0; i < indexNum; ++i){
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
            // Declare bitmap array
            RoaringBitmap[] bitmaps = new RoaringBitmap[splitNum];
            for(int j = 0; j < splitNum; ++j){
                bitmaps[j] = new RoaringBitmap();
            }
            indexes.add(bitmaps);
        }
        this.attrSplitPoint = splitPoints;
    }


    public final List<Long> getMinValues() {
        return minValues;
    }

    public final List<Long> getMaxValues() {
        return maxValues;
    }

    /**
     * Insert a record into the rough index in the buffer <br>
     * step 1: updates the maximum and minimum values
     * step 2: updates the rough index
     * @param triple        fast temporary triple
     * @param recordId      The position where this record is stored in this buffer
     */
    public final void insert(FastTemporaryTriple triple, int recordId){
        final long[] attrValues = triple.attrValues();
        // attrValues.length == minValues.size()
        for (int i = 0; i < indexNum; ++i){
            if(attrValues[i] < minValues.get(i)){
                minValues.set(i, attrValues[i]);
            }
            if(attrValues[i] > maxValues.get(i)){
                maxValues.set(i, attrValues[i]);
            }
            // find store roaring bitmap id, then store the indices
            int pos = mapPosition(i, attrValues[i]);
            RoaringBitmap[] rbs = indexes.get(i);
            rbs[pos].add(recordId);
        }
    }

    /**
     * find the map position corresponding to this value
     * @param attrId        i-th index attribute
     * @param value         query value
     * @return              position
     */
    public final int mapPosition(int attrId, long value){
        for(int i = 0; i < splitNum - 1; ++i){
            if(value < attrSplitPoint.get(attrId)[i]){
                return i;
            }
        }
        return splitNum - 1;
    }

    /**
     * Determine which bitmaps are included based on the query scope
     * @param attrId        query attribute id
     * @param min           query min value
     * @param max           query max value
     * @return              startPos and endPos
     */
    public final int[] mapRange(int attrId, long min, long max){
        int start = -1;
        int end = -1;
        boolean setStart = false;
        for(int i = 0; i < splitNum - 1; ++i){
            if(!setStart && min < attrSplitPoint.get(attrId)[i]){
                start = i;
                setStart = true;
            }
            if(max < attrSplitPoint.get(attrId)[i]){
                end = i;
                break;
            }
        }
        if(start == -1){
            start = end = splitNum - 1;
        }else if(end == -1){
            end = splitNum - 1;
        }
        return new int[]{start, end};
    }

    /**
     * Returns a bitmap of records that meet the criteria
     * @param attrId        query attribute id
     * @param min           query min value
     * @param max           query max value
     * @return              bitmap
     */
    public final RoaringBitmap rangeQuery(int attrId, long min, long max){
        RoaringBitmap ans = new RoaringBitmap();
        if(max < minValues.get(attrId) || min > maxValues.get(attrId)){
            return ans;
        }

        final int[] pos = mapRange(attrId, min, max);
        for(int i = pos[0]; i <= pos[1]; ++i){
            ans.or(indexes.get(attrId)[i]);
        }
        return ans;
    }

    public final RoaringBitmap query(List<IndependentConstraintQuad> icQuads){
        RoaringBitmap rb = new RoaringBitmap();
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

        if(!earlyBreak){
            boolean first = true;
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                RoaringBitmap curRB = rangeQuery(idx, min, max);
                if(first){
                    rb = curRB;
                    first = false;
                }else{
                    rb.and(curRB);
                }
            }
        }

        return rb;
    }


    public static void main(String[] args){
        List<Long[]> splitPoints = new ArrayList<>(2);
        Long[] splits1 = {10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L};
        Long[] splits2 = {10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L};

        final int recordNum = 100;

        splitPoints.add(splits1);
        splitPoints.add(splits2);
        RoughIndex testIndex = new RoughIndex(2, splitPoints);
        Random r = new Random();
        List<FastTemporaryTriple> triples = new ArrayList<>(recordNum);
        //record FastTemporaryTriple(long timestamp, RID rid, long[] attrValues)
        for(int i = 0; i < recordNum; ++i){
            long[] attrValues = new long[]{r.nextInt(100), r.nextInt(100)};
            FastTemporaryTriple triple = new FastTemporaryTriple(i, null, attrValues);
            triples.add(triple);
            testIndex.insert(triple, i);
        }

        // 5 <= attr1 <= 25 AND 5 <= attr2 <= 25
        RoaringBitmap rb1 = testIndex.rangeQuery(0, 0, 19);
        RoaringBitmap rb2 = testIndex.rangeQuery(1, 0, 19);
        rb1.and(rb2);
        for(int rowId : rb1){
            FastTemporaryTriple triple = triples.get(rowId);
            StringBuffer output = new StringBuffer(128);
            output.append("rowId: ").append(rowId);
            output.append(" time: ").append(triple.timestamp());
            output.append(" attr0: ").append(triple.attrValues()[0]);
            output.append(" attr1: ").append(triple.attrValues()[1]);
            System.out.println(output);
        }
    }
}
