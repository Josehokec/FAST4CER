package BPlusTree;

import Common.IndexValuePair;
import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Serializer;

import java.io.File;
import java.util.List;

/**
 * BPlusTree, store in Disk
 */
public class DiskBPlusTree implements BPlusTreeInterface{

    private BPlusTree<Long, String> bPlusTree;

    DiskBPlusTree(){
        String dir = System.getProperty("user.dir");
        String indexDirectory = dir + File.separator + "src" + File.separator + "out";
        bPlusTree = BPlusTree.file().directory(indexDirectory)
                        .maxLeafKeys(128).maxNonLeafKeys(128).
                        segmentSizeMB(1).keySerializer(Serializer.LONG).valueSerializer(Serializer.utf8()).naturalOrder();
    }

    @Override
    public void insert(long key, IndexValuePair value) {
        String storeStr = value.toString();
        bPlusTree.insert(key, storeStr);
    }

    @Override
    public List<IndexValuePair> rangeQuery(long min, long max) {
        return null;
    }
}
