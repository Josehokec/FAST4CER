package Store;

/**
 * RID = <page, offset>
 */
public record RID(int page, int offset) implements Comparable<RID> {

    @Override
    public String toString() {
        return "page: " + page + " offset: " + offset;
    }

    @Override
    public int compareTo(RID o) {
        if (this.page() != o.page()) {
            return this.page() - o.page();
        } else {
            return this.offset() - o.offset();
        }
    }
}

