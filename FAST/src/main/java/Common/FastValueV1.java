package Common;

import Method.IthRID;
import Store.EventStore;
import Store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 只索引用List不用[]，是为了后续可能还有其他属性的索引加入
 * 除开最大最小值，还可以加取值范围列表，目前没有实现
 * FastValue由RangeBitmap + TsRight-Time-Block + RID 组成
 * 注意RangeBitmap只能一批一批数据插入
 */
public class FastValueV1 {
    private final List<Long> minValues;               // Attribute synopsis 每个属性对应的最小值
    private final List<Long> maxValues;               // Attribute synopsis 每个属性对应的最大值
    private final List<RangeBitmap> RBList;           // BRB 为每个属性构建范围位图
    private List<byte[]> lowBitTsList;                // 低位的时间戳列表
    private List<RID> ridList;                        // PRB rid列表

    public FastValueV1(int indexAttrNum){
        RBList = new ArrayList<>(indexAttrNum);
        minValues = new ArrayList<>(indexAttrNum);
        maxValues = new ArrayList<>(indexAttrNum);
        // 设置最大值最小值
        for(int i = 0; i < indexAttrNum; ++i){
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
        }
    }

    /**
     * 对某个索引的属性列进行范围查找
     * @param idx 索引属性列编号
     * @param min 最小值
     * @param max 最大值
     * @return 满足条件的位图
     */
    public final RoaringBitmap betweenQuery(int idx, long min, long max){
        if(minValues.get(idx) > max || maxValues.get(idx) < min){
            return null;
        }
        return RBList.get(idx).between(min, max);
    }

    /**
     * 对某个索引的属性列进行小于一个最大值的范围查找
     * @param idx 索引属性列编号
     * @param max 最大值
     * @return 满足条件的位图
     */
    public final RoaringBitmap lteQuery(int idx, long max){
        if(minValues.get(idx) > max){
            return null;
        }
        return RBList.get(idx).lte(max);
    }

    /**
     * 对某个索引的属性列进行范围查找
     * @param idx 索引属性列编号
     * @param min 最小值
     * @return 满足条件的位图
     */
    public final RoaringBitmap gteQuery(int idx, long min){
        if(maxValues.get(idx) < min){
            return null;
        }
        return RBList.get(idx).gte(min);
    }

    /**
     * 根据RoaringBitmap的值去得到时间戳
     * @param rb RoaringBitmap
     * @return 时间戳列表
     */
    public final List<Long> getTimestampUsingRB(RoaringBitmap rb, long tsLeft, int rightBitLen){
        if(rb == null){
            return null;
        }

        List<Long> ansList = new ArrayList<>();
        rb.forEach((Consumer<? super Integer>) i ->{
            long ts = (tsLeft << rightBitLen) + Converter.bytesToLong(lowBitTsList.get(i));
            ansList.add(ts);
        });
        return ansList;
    }

    /**
     * 最小选择率的记录元组
     * @param rb                满足所有独立谓词约束的位图
     * @param store             存储记录的store
     * @param rightBitLen       低位的时间戳比特长度
     * @param pos               最小的索引位置
     * @param highBitTs         高位时间戳
     * @param tau               查询事件窗口
     * @param highBitRB         高位时间戳位图
     * @param replayInterval    回放间隙
     * @return                  满足条件的记录集合
     */
    public final List<byte[]> getBytesRecordUsingRB(RoaringBitmap rb, EventStore store, int rightBitLen, int pos,
                                              long highBitTs, long tau, Roaring64Bitmap highBitRB,
                                              Roaring64Bitmap replayInterval){

        List<byte[]> ans = new ArrayList<>();
        rb.forEach((Consumer<? super Integer>) i ->{
            RID rid = ridList.get(i);
            // 得到这个记录的时间戳
            long ts = (highBitTs << rightBitLen) + Converter.bytesToLong(lowBitTsList.get(i));
            long minTs, maxTs;
            if(pos == -1){
                minTs = ts;
                maxTs = ts + tau;
            } else if (pos == 0) {
                minTs = Math.max(0, (ts - tau));
                maxTs = ts + tau;
            } else if (pos == 1) {
                minTs = Math.max(0, (ts - tau));
                maxTs = ts + tau;
            }else{
                // pos只能是-1 0 1
                throw new RuntimeException("the value of pos has error");
            }
            // 生成replay interval
            replayInterval.addRange(minTs, maxTs);

            long minHighTs = minTs >> rightBitLen;
            long maxHighTs = maxTs >> rightBitLen;
            if(minHighTs != highBitTs){
                highBitRB.addRange(minHighTs, highBitTs);
            }
            if(maxHighTs != highBitTs){
                highBitRB.addRange(highBitTs, maxHighTs);
            }

            // 根据RID读取记录
            ans.add(store.readByteRecord(rid));
        });
        return ans;
    }


    /**
     * 保证记录的时间戳在这个回放间隙里面就ok了
     * @param rb                满足所有独立谓词约束的位图
     * @param store             存储记录的store
     * @param rightBitLen       低位的时间戳比特长度
     * @param highBitTs         高位时间戳
     * @param replayInterval    回放间隙
     * @return                  满足条件的记录集合
     */
    public final List<byte[]> getBytesRecordUsingRB(RoaringBitmap rb, EventStore store, int rightBitLen,
                                              long highBitTs, Roaring64Bitmap replayInterval){

        List<byte[]> ans = new ArrayList<>();
        rb.forEach((Consumer<? super Integer>) i ->{
            RID rid = ridList.get(i);
            // 得到这个记录的时间戳
            long ts = (highBitTs << rightBitLen) + Converter.bytesToLong(lowBitTsList.get(i));
            if(replayInterval.contains(ts)){
                // 根据RID读取记录
                ans.add(store.readByteRecord(rid));
            }
        });
        return ans;
    }

    /**
     * 只生成高位时间戳
     * @param rb        位图
     * @param store     存储仓库
     * @param rightBitLen
     * @param pos
     * @param highBitTs
     * @param tau
     * @param highBitRB
     * @return
     */
    public final List<byte[]> getBytesRecordUsingRB(RoaringBitmap rb, EventStore store, int rightBitLen, int pos,
                                                    long highBitTs, long tau, Roaring64Bitmap highBitRB){

        List<byte[]> ans = new ArrayList<>();

        int len = rb.getCardinality();
        long t0 = (highBitTs << rightBitLen);

        List<Long> replayStarts = new ArrayList<>(len);
        List<Long> replayEnds = new ArrayList<>(len);

        for(int indices : rb){
            RID rid = ridList.get(indices);
            // 根据RID读取记录
            ans.add(store.readByteRecord(rid));

            long minTs, maxTs;
            long curLowBitTs = Converter.bytesToLong(lowBitTsList.get(indices));
            switch (pos) {
                case -1 -> {
                    minTs = curLowBitTs;
                    maxTs = curLowBitTs + tau;
                }
                case 0 -> {
                    minTs = curLowBitTs - tau;
                    maxTs = curLowBitTs + tau;
                }
                default -> {
                    minTs = curLowBitTs - tau;
                    maxTs = curLowBitTs;
                }
            }

            replayStarts.add(minTs);
            replayEnds.add(maxTs);
        }

        long earlyHighBitTs = (t0 + replayStarts.get(0)) >> rightBitLen;
        long lateHighBitTs = (t0 + replayEnds.get(len - 1)) >> rightBitLen;
        if(earlyHighBitTs != lateHighBitTs){
            highBitRB.addRange(earlyHighBitTs, lateHighBitTs + 1);
        }else{
            highBitRB.add(earlyHighBitTs);
        }

        //

        return ans;
    }

    /**
     * 使用位图来获取相关的字节记录
     * @param rb        位图
     * @param store     存储记录
     * @return          字节记录
     */
    public final List<byte[]> getBytesRecordUsingRB(RoaringBitmap rb, EventStore store){
        List<byte[]> ans = new ArrayList<>();
        rb.forEach((Consumer<? super Integer>) i ->{
            RID rid = ridList.get(i);
            // 根据RID读取记录
            ans.add(store.readByteRecord(rid));
        });
        return ans;
    }

    /**
     * 直接返回带编号的RID列表,之后再读取字节记录,这样做比根据rid读字节记录会快
     * @param rb        位图
     * @param ith       查询的第几个变量
     * @return          RIDList
     */
    public final List<IthRID> getRIDList(RoaringBitmap rb, int ith){
        final int len = rb.getCardinality();
        List<IthRID> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i ->{
            RID rid = ridList.get(i);
            ans.add(new IthRID(rid, ith));
        });
        return ans;
    }

    /**
     * 为一批数据构造范围位图
     * 存储的值和实际记录的值不完全相同，因为这里接收到的值已经被预处理了
     * @param batchAttrValues 一批的值
     * @param maxRangeArray 这个属性对应的最大范围
     */
    public void insertBatch(List<FastValueQuad> batchAttrValues, long[] maxRangeArray){
        int indexAttrNum = minValues.size();
        int batchSize = batchAttrValues.size();
        lowBitTsList = new ArrayList<>(batchSize);
        ridList = new ArrayList<>(batchSize);

        // 能有最大范围最好了，可以节省空间
        // 设置属性最大值与最小值
        RangeBitmap.Appender[] appends = new RangeBitmap.Appender[indexAttrNum];

        for(int i = 0; i < indexAttrNum; ++i){
            appends[i] = RangeBitmap.appender(maxRangeArray[i]);
        }

        for(FastValueQuad tripe : batchAttrValues){
            long[] values = tripe.getAttrValues();
            for(int i = 0; i < values.length; ++i){
                long value = values[i];
                appends[i].add(value);
                // 设置FastValue的最大最小值
                if(value < minValues.get(i)){
                    minValues.set(i, value);
                }
                if(value > maxValues.get(i)){
                    maxValues.set(i, value);
                }
            }
            lowBitTsList.add(tripe.getTsRight());
            ridList.add(tripe.getRid());
        }
        //构建范围位图
        for(int i = 0; i < indexAttrNum; ++i){
            RBList.add(appends[i].build());
        }
    }
}
