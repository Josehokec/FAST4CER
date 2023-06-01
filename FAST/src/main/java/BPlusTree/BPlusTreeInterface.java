package BPlusTree;

import Common.IndexValuePair;

import java.util.List;

public interface BPlusTreeInterface{

    public abstract void insert(long key, IndexValuePair value);

    public abstract List<IndexValuePair> rangeQuery(long min, long max);
}

