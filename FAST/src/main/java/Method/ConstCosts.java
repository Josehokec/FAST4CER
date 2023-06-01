package Method;

import java.util.List;

public class ConstCosts {
    public static double C_PAGE;                // time cost of reading one page from disk
    public static double C_SORT;                // time cost of sorting
    public static double C_TREE;                // time cost of reading a record using b plus tree
    public static double C_LIST;                // time cost of reading a record using skip list
    public static double C_MERGE;               // time cost of merging
    public static double C_BMP;                // time cost of range querying for single element using range bitmap
    public static double C_AND;                 // time cost of performing AND operations on two bitmaps
    public static double C_GET;                 // time cost of getting a rid using roaring bitmap

    /**
     * Here is a rough cost evaluation, noting that these cost constants are different for different systems,<br>
     * in fact, these cost constants will increase as the data volume increases. <br>
     * Therefore, this cost constant is not accurate <br>
     * time unit: ns
     */
    public static void setConstCosts(){
        C_SORT = 17.7839;
        C_TREE = 18.4235;
        C_LIST = 8.1632;
        C_MERGE = 27.4494;
        C_PAGE = 817135.2500;
        // 下面三个参数是用来估算FAST的代价的，但是目前不用
        C_BMP = 0;
        C_AND = 0;
        C_GET = 0;
    }

    /**
     * costs1: C_SORT, C_TREE, C_SKIP, C_MERGE, C_PAGE
     * C_BMP | C_AND | C_GET
     * FAST some event store in memory without index
     */
    public static void setConstCosts(double[] costs1, double[] costs2){
        if(costs1.length != 5 || costs2.length != 3){
            throw new RuntimeException("args error, only need 8 args.");
        }
        // C_SORT, C_TREE, C_SKIP, C_MERGE, C_PAGE
        C_SORT = costs1[0];
        C_TREE = costs1[1];
        C_LIST = costs1[2];
        C_MERGE = costs1[3];
        C_PAGE = costs1[4];

        C_BMP = costs2[0];
        C_AND = costs2[1];
        C_GET = costs2[2];
    }

    public static void setIntervalScanCosts(List<Double> costs1){
        if(costs1.size() != 5){
            throw new RuntimeException("args error, only need 8 args.");
        }
        // C_SORT, C_TREE, C_SKIP, C_MERGE, C_PAGE
        C_SORT = costs1.get(0);
        C_TREE = costs1.get(1);
        C_LIST = costs1.get(2);
        C_MERGE = costs1.get(3);
        C_PAGE = costs1.get(4);
    }

    public static void setBitmapCosts(List<Double> costs2){
        if(costs2.size() != 3){
            throw new RuntimeException("args error, only need 8 args.");
        }
        C_BMP = costs2.get(0);
        C_AND = costs2.get(1);
        C_GET = costs2.get(2);
    }
}
