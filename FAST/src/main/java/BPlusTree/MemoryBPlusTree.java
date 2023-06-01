package BPlusTree;

import Common.IndexValuePair;
import xyz.proadap.aliang.BPlusTree;

import java.util.List;

public class MemoryBPlusTree implements BPlusTreeInterface{
    private BPlusTree bPlusTree;

    public MemoryBPlusTree(){
        bPlusTree = new BPlusTree(64);
    }

    @Override
    public void insert(long key, IndexValuePair value) {
        bPlusTree.insert(key, value);
    }

    @Override
    public List<IndexValuePair> rangeQuery(long min, long max) {
        if(max == Long.MAX_VALUE){
            return bPlusTree.rangeQuery(min, max);
        }else{
            return bPlusTree.rangeQuery(min, max + 1);
        }
    }
}
