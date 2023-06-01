package Common;

import java.util.ArrayList;
import java.util.List;

public class TimeBlockList {
    private List<Long[]> blockList;
    private int usedBlockID;
    private int usedOffset;

    private int perPageTimestampNum;

    public TimeBlockList(){
        perPageTimestampNum = 1024;
        Long[] firstBlock = createNewBlock(perPageTimestampNum);
        blockList = new ArrayList<Long[]>();
        blockList.add(firstBlock);
        usedBlockID = 0;
        usedOffset = 0;
    }

    /*
    当能够预估要存储的时间戳数量时候
    调用这个方法能够避免频繁的扩容操作
     */
    public TimeBlockList(int capacity, int perPageTimestampNum){
        this.perPageTimestampNum = perPageTimestampNum;
        Long[] firstBlock = createNewBlock(perPageTimestampNum);
        blockList = new ArrayList<Long[]>(capacity);
        blockList.add(firstBlock);
        usedBlockID = 0;
        usedOffset = 0;
    }

    /*
    向时间块插入一个时间戳
     */
    public boolean insertTimestamp(long t, int indices){
        int curIndices = usedBlockID * perPageTimestampNum + usedOffset;
        if(curIndices != indices){
            System.out.println("indices bugs");
            return false;
        }

        blockList.get(usedBlockID)[usedOffset++] = t;
        if(usedOffset == perPageTimestampNum){
            usedOffset = 0;
            ++usedBlockID;
            blockList.add(createNewBlock(perPageTimestampNum));
        }
        return true;
    }

    /*
    插入一大批时间戳，使用System.arrayCopy加速
     */
    public boolean batchInsertTimestamp(Long[] ts){
        int batchSize = ts.length;

        return true;
    }

    /*
    如果一个块存1024个时间戳，时间戳用long存储的话，一个块占8KB
     */
    public Long[] createNewBlock(int perPageTimestampNum){
        return new Long[perPageTimestampNum];
    }

    /*
    bitmap返回的是记录存储索引，我们需要根据它来访问到具体的时间戳
     */
    public long getTiemstampByRecordID(long recordID){
        int offset = (int) (recordID % perPageTimestampNum);
        int blockID = (int) (recordID / perPageTimestampNum);
        return blockList.get(blockID)[offset];
    }

    public long getRecordSumNum(){
        return usedBlockID * perPageTimestampNum + usedOffset;
    }
}
