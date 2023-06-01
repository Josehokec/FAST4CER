package Common;

import java.util.ArrayList;
import java.util.List;

/**
 * ReplayIntervals <br>
 * interval [start, end] <br>
 * Note that each replay interval cannot intersect<br>
 * here we use a trick to filter more events<br>
 * for example, suppose variable a generates 10 intervals: 1-10<br>
 * however, variable b only can be find in interval 4, 5, and 6<br>
 * then next we find variable c we can only visit 4,5 and 6<br>
 */
public class ReplayIntervals {
    private int intervalNum;                    // 间隙数量
    private int  ptr;                           // pointer to i-th interval
    private final List<Long> intervals;         // i-th interval = (intervals[2 * i], intervals[2 * i + 1])
    private List<Boolean> preHits;              // (accelerate trick) whether other variables hit the interval
    private List<Boolean> curHits;              // (accelerate trick) whether other variables hit the interval
    public ReplayIntervals(){
        intervals = new ArrayList<>(64);
        preHits = new ArrayList<>(64);
        ptr = 0;
        intervalNum = 0;
    }

    /**
     * Be sure to insert it in sequence, otherwise this class will have problems <br>
     * Notice: insert an interval, end - start is same <br>
     * That is to say, start must be greater than or equal to lastStart
     * @param start     interval start time
     * @param end       interval end time
     */
    public final void insert(long start, long end){
        if(start > end){
            throw new RuntimeException("start time greater than end time, insert illegally");
        }

        if(intervalNum == 0){
            intervals.add(start);
            intervals.add(end);
            preHits.add(true);
            intervalNum++;
        }else{
            int idx = 2 * (intervalNum - 1);
            long lastStart = intervals.get(idx);
            long lastEnd = intervals.get(idx + 1);
            if(start < lastStart){
                throw new RuntimeException("insert illegally");
            }
            // if intervals intersect, then need to merge the replay intervals, notice there no end > lastEnd
            if(start <= lastEnd){
                intervals.set(idx + 1, Math.max(lastEnd, end));
            }else{
                intervals.add(start);
                intervals.add(end);
                preHits.add(true);
                intervalNum++;
            }
        }
    }

    /**
     * Determine if a timestamp is included in replay intervals
     * This function uses pointers and can only be read sequentially without rewinding
     * @param x         timestamp
     * @return          If x is in the interval, return true, otherwise return false
     */
    public final boolean include(long x){
        if(curHits == null){
            curHits = new ArrayList<>(intervalNum);
            for(int i = 0; i < intervalNum; ++i){
                curHits.add(false);
            }
        }

        if(intervalNum == 0){
            return false;
        }

        // try to find x > interval.start position
        while(ptr + 1 < intervalNum && x > intervals.get((ptr << 1) + 1)){
            // pointer moves to the next interval
            ptr++;
        }
        int startPos = ptr << 1;
        long start = intervals.get(startPos);
        long end = intervals.get(startPos + 1);
        //System.out.println("x: " + x + " ptr: " + ptr + " start: "  + start + " end: " + end + " preHit: " + preHits.get(ptr));

        if(x <= end && x >= start && preHits.get(ptr)){
            // variable hit the replay intervals
            curHits.set(ptr, true);
            return true;
        }else{
            return false;
        }
    }

    /**
     * when checking a new variable<br>
     * a rewind operation is required
     */
    public final void rewind(){
        ptr = 0;
        // update hits, notice first rewind curHits is null
        if(curHits != null){
            preHits = new ArrayList<>(curHits);
            curHits = null;
        }
    }

    /**
     * Return all replay intervals
     * @return  intervals list (start, end)
     */
    public final List<long[]> getAllReplayIntervals(){
        List<long[]> ans = new ArrayList<>(intervalNum);
        for(int i = 0; i < intervalNum; ++i){
            int startPos = (i << 1);
            long[] curInterval = {intervals.get(startPos), intervals.get(startPos + 1)};
            ans.add(curInterval);
        }
        return ans;
    }

    /**
     * Check if all intervals do not intersect
     * @return  if intersect then return false，otherwise return true
     */
    public final boolean check(){
        long preEnd = Long.MIN_VALUE;
        for(int i = 0; i < intervalNum; ++i){
            if(intervals.get(i << 1) > preEnd){
                preEnd = intervals.get(i << 1);
            }else{
                return false;
            }
        }
        return true;
    }

    public final void print(){
        for(int i = 0; i < intervalNum; ++i){
            String buff = "start: " + intervals.get(i << 1) + " end: " + intervals.get(1 + (i << 1));
            System.out.println(buff);
        }
        System.out.println("ptr: " + ptr);
    }

    public static void main(String[] args){
        long[] starts1 = {1, 1, 2, 6, 8, 11};
        long[] ends1 = {3, 2, 4, 8, 10, 12};
        ReplayIntervals intervals1 = new ReplayIntervals();
        for(int i = 0; i < starts1.length; ++i){
            intervals1.insert(starts1[i], ends1[i]);
        }
        intervals1.print();
        long[] queries = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        for (long query : queries) {
            System.out.println("include " + query + "? " + intervals1.include(query));
        }
        intervals1.print();
    }
}

/**
 long[] starts1 = {1, 5, 9, 11};
 long[] ends1 = {4, 8, 12, 14};
 ReplayIntervals intervals1 = new ReplayIntervals();

 for(int i = 0; i < starts1.length; ++i){
 intervals1.insert(starts1[i], ends1[i]);
 }
 intervals1.print();
 long[] queries = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

 for (long query : queries) {
 System.out.println("include " + query + "? " + intervals1.include(query));
 }
 intervals1.print();
 System.out.println("check? " + intervals1.check());

 long[] starts2 = {1, 5, 9, 11, 12};
 long[] ends2 = {3, 7, 11, 13, 14};
 ReplayIntervals intervals2 = new ReplayIntervals();

 for(int i = 0; i < starts2.length; ++i){
 intervals2.insert(starts2[i], ends2[i]);
 }
 intervals2.print();

 long[] queries1 = {0, 1, 2, 3, 4, 10, 14, 15};
 long[] queries2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

 for (long query : queries1) {
 System.out.println("include " + query + "? " + intervals2.include(query));
 }

 intervals2.rewind();
 for (long query : queries2) {
 System.out.println("include " + query + "? " + intervals2.include(query));
 }
 intervals2.print();
 */
