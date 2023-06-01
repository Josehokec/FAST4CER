package FastModule;

import Condition.IndependentConstraintQuad;
import Store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

import static FastModule.RoughIndex.splitNum;

public class EventBuffers {
    public static double factor = 0.15;
    private int recordNum;
    private int indexNum;
    private int flushThreshold;
    private List<Long> minValues;                               // index attribute minValues
    private List<Long> attrMaxRange;                            // index attribute maxValues
    private List<Long[]> attrSplitPoint;                        // split point for rough index
    private HashMap<String, List<FastTemporaryTriple>> buffers; // buffers
    private HashMap<String, RoughIndex> roughIndexes;

    public final int getRecordNum(){
        return recordNum;
    }

    /**
     * Construct function
     * @param indexNum      number of index attribute
     * @param minValues     minimum value for each index attribute
     * @param attrMaxRange  maximum value for each index attribute
     */
    public EventBuffers(int indexNum, List<Long> minValues, List<Long> attrMaxRange){
        recordNum = 0;
        this.indexNum = indexNum;
        flushThreshold = 42 * 1024;
        buffers = new HashMap<>();
        roughIndexes = new HashMap<>();
        this.minValues = minValues;
        this.attrMaxRange = attrMaxRange;
        attrSplitPoint = setSplitPoint(minValues, attrMaxRange);
    }

    // 最后需要根据数据分区的事件类型，加到partitionTable里面
    public final IndexPartition insertRecord(String eventType, FastTemporaryTriple triple){
        IndexPartition partition = null;
        if(buffers.containsKey(eventType)){
            List<FastTemporaryTriple> triples = buffers.get(eventType);
            RoughIndex index = roughIndexes.get(eventType);
            index.insert(triple, triples.size());
            triples.add(triple);
        }else{
            List<FastTemporaryTriple> triples = new ArrayList<>(1024);
            RoughIndex index = new RoughIndex(indexNum, attrSplitPoint);
            // in this bitmap row id
            index.insert(triple, triples.size());
            triples.add(triple);

            buffers.put(eventType, triples);
            roughIndexes.put(eventType, index);
        }
        recordNum++;

        if(recordNum >= flushThreshold){
            // infoMap, bitmaps, timestamps, rids
            HashMap<String, SegmentInfo> infoMap = new HashMap<>();
            RangeBitmap.Appender[] appends = new RangeBitmap.Appender[indexNum];
            if(attrMaxRange != null){
                for(int i = 0; i < indexNum; ++i){
                    appends[i] = RangeBitmap.appender(attrMaxRange.get(i));
                }
            }else{
                for(int i = 0; i < indexNum; ++i){
                    appends[i] = RangeBitmap.appender(Long.MAX_VALUE >> 1);
                }
            }
            List<Long> timestamps = new ArrayList<>(recordNum);
            List<RID>  rids = new ArrayList<>(recordNum);

            int curEventTypeNum = buffers.size();
            double threshold = factor * recordNum / curEventTypeNum;
            int storeRecordNum = 0;

            // construct info, range bitmap, timestamps, rids
            Iterator<Map.Entry<String, List<FastTemporaryTriple>>> iterator = buffers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<FastTemporaryTriple>> entry = iterator.next();
                String key = entry.getKey();
                List<FastTemporaryTriple> value = entry.getValue();
                int size = value.size();
                if(size >= threshold){
                    // flush, build range bitmap
                    int startPos = storeRecordNum;
                    int offset = size;
                    long startTime = value.get(0).timestamp();
                    long endTime = value.get(size - 1).timestamp();
                    RoughIndex roughIndex = roughIndexes.get(key);
                    List<Long> minValues = roughIndex.getMinValues();
                    List<Long> maxValues = roughIndex.getMaxValues();

                    SegmentInfo info = new SegmentInfo(startPos, offset, startTime, endTime, minValues, maxValues);
                    infoMap.put(key, info);

                    for(FastTemporaryTriple fastTemporaryTriple : value){
                        long[] attrValues = fastTemporaryTriple.attrValues();
                        for(int k = 0; k < indexNum; ++k){
                            appends[k].add(attrValues[k]);
                        }
                        timestamps.add(fastTemporaryTriple.timestamp());
                        rids.add(fastTemporaryTriple.rid());
                    }

                    // clear buffer
                    iterator.remove();
                    storeRecordNum += size;
                }
            }

            // Construct range bitmap
            List<RangeBitmap> rangeBitmaps = new ArrayList<>(indexNum);
            for(int i = 0; i < indexNum; ++i){
                rangeBitmaps.add(appends[i].build());
            }

            // build partition
            partition = new IndexPartition();
            partition.setInfoMap(infoMap);
            partition.setRangeBitmapList(rangeBitmaps);
            partition.setTimestamps(timestamps);
            partition.setRids(rids);

            recordNum -= storeRecordNum;
        }

        return partition;
    }

    /**
     * according minValues and max values to determine split point
     * @param minValues     index attribute minValues
     * @param attrMaxRange  index attribute range
     */
    public List<Long[]> setSplitPoint(List<Long> minValues, List<Long>  attrMaxRange){
        if(minValues.size() != indexNum){
            throw new RuntimeException("index attribute num not equal to the length of minValues");
        }
        List<Long[]> attrSplitPoint = new ArrayList<>(indexNum);
        for(int i = 0; i < indexNum; ++i){
            long width = attrMaxRange.get(i) / splitNum;
            Long[] splitPoints = new Long[splitNum - 1];
            for(int j = 0; j < splitNum - 1; ++j){
                splitPoints[j] = minValues.get(i) + width * (j + 1);
            }
            attrSplitPoint.add(splitPoints);
        }
        return attrSplitPoint;
    }

    /**
     * filter out relevant records that meet the conditions in the buffer
     * @param eventType     event type
     * @param icQuads       independent predicate constraints
     * @return              <rid, timestamp> pair
     */
    public List<RidTimePair> query(String eventType, List<IndependentConstraintQuad> icQuads){
        RoughIndex index = roughIndexes.get(eventType);
        // step 1: rough filtration
        RoaringBitmap rb = index.query(icQuads);
        // avoid multiple resize
        final int len = rb.getCardinality();
        List<RidTimePair> ans = new ArrayList<>(len);
        List<FastTemporaryTriple> triples = buffers.get(eventType);
        // step 2: accurately filter in rough filtered record
        for(int i : rb){
            FastTemporaryTriple triple = triples.get(i);
            boolean satisfy = true;
            long[] attrValues = triple.attrValues();
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                if(attrValues[idx] < min || attrValues[idx] > max){
                    satisfy = false;
                    break;
                }
            }
            if(satisfy){
                ans.add(new RidTimePair(triple.rid(), triple.timestamp()));
            }
        }

        return ans;
    }

}
/*
for (Map.Entry <String, List<FastTemporaryTriple>> entry: buffers.entrySet()) {
            String key = entry.getKey();
            List<FastTemporaryTriple> value = entry.getValue();
            int size = value.size();
            if(size >= threshold){
                // flush, build range bitmap
                int startPos = storeRecordNum;
                int offset = size;
                long startTime = value.get(0).timestamp();
                long endTime = value.get(size - 1).timestamp();
                RoughIndex roughIndex = roughIndexes.get(key);
                List<Long> minValues = roughIndex.getMinValues();
                List<Long> maxValues = roughIndex.getMaxValues();

                SegmentInfo info = new SegmentInfo(startPos, offset, startTime, endTime, minValues, maxValues);
                infoMap.put(key, info);

                for(FastTemporaryTriple fastTemporaryTriple : value){
                    long[] attrValues = fastTemporaryTriple.attrValues();
                    for(int k = 0; k < indexNum; ++k){
                        appends[k].add(attrValues[k]);
                    }
                    timestamps.add(fastTemporaryTriple.timestamp());
                    rids.add(fastTemporaryTriple.rid());
                }

                // clear buffer
                buffers.remove(key);
                storeRecordNum += size;
            }
        }
 */
