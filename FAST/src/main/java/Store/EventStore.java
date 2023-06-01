package Store;

import Common.Converter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事件存储类
 * 存储字节类型的记录
 * 文件名字=schemaName.store
 * 没有实现缓存功能
 */
public class EventStore {
    private final int recordSize;                   // 一条记录所占字节大小
    private int curPage;                            // 当前缓冲的页
    private final int pageSize;                     // 页大小 越大查询开销越小
    private int pageNum;                            // 上次插入的记录所在页号
    private int offset;                             // 偏移
    private final String schemaName;                // 模式名字
    private File file;                              // 存储文件
    private final ByteBuffer buf;                   // 字节缓存
    private MappedByteBuffer readMappedBuffer;      // 只读取一页存放在内存中

    public EventStore(String schemaName, int recordSize){
        String dir = System.getProperty("user.dir");
        String storePath = new File(dir).getParent() + File.separator + "store";
        String filename = schemaName + ".binary_file";
        String filePath = storePath + File.separator + filename;
        System.out.println("store filePath: " + filePath);
        file = new File(filePath);
        // 之前存在过这个文件 要把内容清了
        if(file.exists()){
            if(file.delete()){
                System.out.println("file: '"+ filename + "' exists in disk, we clear the file content.");
            }
        }

        this.recordSize = recordSize;
        this.schemaName = schemaName;
        // 申请一个页面8K
        pageSize = 8 * 1024;
        buf = ByteBuffer.allocate(pageSize);
        curPage = -1;
        pageNum = 0;
        offset = 0;
        readMappedBuffer = null;
    }

    /**
     * 得到一个页面的大小
     * @return  页面的大小
     */
    public final int getPageSize(){
        return pageSize;
    }

    public int getPageNum(){
        return pageNum;
    }

    /**
     * 插入一条记录的到文件中
     * byte[] content = new byte[len];
     * System.arraycopy(array, 0, content, 0, len);
     * @param record 字节数组类型的记录
     * @return RID值
     */
    public final RID insertByteRecord(byte[] record){
        RID rid;
        // 缓存再也放不下数据了，则将数据刷到文件中
        if(offset + recordSize > pageSize){
            // 把内容锁住，然后将数据刷到文件中，
            buf.flip();
            try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
                //注意到这个页面可能没有满，但是无所谓
                byte[] array = buf.array();
                out.write(array);
                out.flush();
            }catch (Exception e) {
                e.printStackTrace();
            }
            pageNum++;
            offset = 0;
        }
        buf.put(record);
        rid = new RID(pageNum, offset);
        offset += recordSize;
        return rid;
    }

    /**
     * 根据rid的值去读字节
     * 有可能当前页之前读取过，那么就没必要再次映射了
     * if rid.page = curPage 直接读取就行了
     * @param rid RID值
     * @return 读取到的记录
     */
    public final byte[] readByteRecord(RID rid){
        // 读之前必须保证缓冲里面的数据全写在文件里了
        if(buf.hasRemaining()){
            buf.flip();
            int len = buf.limit();
            try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
                byte[] array = buf.array();
                byte[] content = new byte[len];
                System.arraycopy(array, 0, content, 0, len);
                out.write(content);
                out.flush();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        // System.out.println(file.getAbsoluteFile());
        byte[] byteRecord = new byte[recordSize];
        // 这个页面已经在内存中了 直接读取然后返回
        if(curPage == rid.page()){
            readMappedBuffer.get(rid.offset(), byteRecord);
            return byteRecord;
        }

        RandomAccessFile raf;
        try{
            int queryPage = rid.page();
            raf = new RandomAccessFile(file, "rw");
            long startPos = (long) queryPage * pageSize;

            //long readStart = System.nanoTime();
            // 获取文件通道，然后将指定位置的数据送到内存中
            FileChannel fileChannel = raf.getChannel();
            readMappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startPos, pageSize);
            curPage = queryPage;
            //long readEnd = System.nanoTime();
            readMappedBuffer.get(rid.offset(), byteRecord);
            //System.out.println("read a page cost: " + (readEnd - readStart) / 1000 + "us");
            // 如果不close可能导致异常
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return byteRecord;
    }

    public final MappedByteBuffer getMappedBuffer(int queryPage){
        if(queryPage > pageNum){
            throw new RuntimeException("this page does not exist!");
        }
        RandomAccessFile raf;
        MappedByteBuffer ans = null;
        try{
            raf = new RandomAccessFile(file, "rw");
            long startPos = (long) queryPage * pageSize;
            FileChannel fileChannel = raf.getChannel();
            ans = fileChannel.map(FileChannel.MapMode.READ_WRITE, startPos, pageSize);
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ans;
    }

    public void print(){
        System.out.println("PageNum: " + pageNum + " Offset: " + offset);
    }

    public static void main(String[] args){
        // 测试读取一个64KB页面开销是多大
        System.out.println("hello world.");
        EventStore store = new EventStore("debug", 4);
        int[] items = {0,1,2,3,4,5,6,7,8,9};
        for(int i = 0; i < items.length; ++i){
            byte[] record = Converter.intToBytes(items[i]);
            RID rid = store.insertByteRecord(record);
            System.out.println("item " + i + " rid: " + rid);
        }

        int[] pages = {0,0,0,0,1,1,1,1,2,2};
        int[] offsets = {0,4,8,12,0,4,8,12,0,4};

        for(int i = 0; i < pages.length; ++i){
            byte[] ans = store.readByteRecord(new RID(pages[i], offsets[i]));
            System.out.println(Converter.bytesToInt(ans));
        }
    }
}

