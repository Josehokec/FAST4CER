package SkipList;

/**
 * ref：<a href="https://www.freesion.com/article/4513366377/">SkipList</a>
 * */
public class SkipListNode <T>{
    public long key;
    public T value;
    public SkipListNode<T> up, down, left, right; // four pointers
    public static final long HEAD_KEY = Long.MIN_VALUE;
    public static final long  TAIL_KEY = Long.MAX_VALUE;
    public SkipListNode(long k,T v) {
        // TODO Auto-generated constructor stub
        key = k;
        value = v;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public boolean equals(Object o) {
        if (this==o) {
            return true;
        }
        if (o==null) {
            return false;
        }
        if (!(o instanceof SkipListNode<?>)) {
            return false;
        }
        SkipListNode<T> ent;
        try {
            ent = (SkipListNode<T>)  o;
        } catch (ClassCastException ex) {
            return false;
        }
        return (ent.getKey() == key) && (ent.getValue() == value);
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "key-value:"+key+"-"+value;
    }
}

