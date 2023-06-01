package SkipList;

import java.util.List;

public class SkipListTest {
    public void simpleTest(){
        SkipList<String> skipList = new SkipList<String>();
        System.out.println(skipList);

        int[] keys = {1, 1, 2, 2, 3, 3, 4, 4, 5, 7};

        for(int i = 0; i < keys.length; ++i){
            skipList.put(keys[i], "record"+ i);
        }

        System.out.println(skipList);
        System.out.println("skip list size: " + skipList.size());

        long[] minValues = {1, 2, 4, 5};
        long[] maxValues = {1, 3, 6, 9};
        int rangeNum = minValues.length;
        for(int i = 0; i < rangeNum; ++i){
            List<String> queryResult1 = skipList.rangeQuery(minValues[i], maxValues[i]);
            System.out.println("query result size: " + queryResult1.size());
            for(String r : queryResult1){
                System.out.println(r);
            }
        }
        System.out.println("finished");
    }

    public void pressureTest(){
        int num = 10000000;
        SkipList<String> skipList = new SkipList<String>();
        SkipList<String> skipList2 = new SkipList<String>();
        SkipList<String> skipList3 = new SkipList<String>();
        for(int i = 0; i < num; ++i){
            skipList.put(i, "record"+ i);
            skipList2.put(i, "record"+ i);
            skipList3.put(i, "record"+ i);
        }

        System.out.println("skip list size: " + skipList.size());
        long start = System.currentTimeMillis();
        List<String> queryResult1 = skipList.rangeQuery(1, 1000000);
        long end = System.currentTimeMillis();
        System.out.println("query result size: " + queryResult1.size());
        System.out.println("query cost: " + (end - start) + "ms");


    }

    public static void main(String[] args) {
        SkipListTest test = new SkipListTest();
        test.simpleTest();
        //test.pressureTest();
    }

}
