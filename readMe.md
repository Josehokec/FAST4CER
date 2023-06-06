# An index Framework for Accelerating Sequential paTtern recognition (FAST)



## Section 1 Running

### Section 1.1 Run Method1: shell script

We have written a shell script ```run.sh``` to let the program run automatically.

You can run the program using the command:

```
./run.sh start test.log
```

If you want to track the output in real-time, you can enter the following command:

```
tail -f test.log
```

**Result explainations**

```
14-th query start...                    // 14-th query pattern
filter cost: 8.602ms                    // index filtering cost
scan cost: 1.782ms                      // disk accessing cost
bucket sizes: [ 4 1441 39 ]             // each bucket size in bucket joining stage
join cost: 0.374ms                      // bucket joining cost
number of tuples: 3                     // number of matched tuples
14-th query pattern time cost: 11.13ms  // sum cost
```

### Section 1.2 Run Method2: IDEA or Esclipes 

You can open ```FAST``` folder with IDEA or Eclipes. Notice that you need to delete part of pom.xml code lines to make it work.

```xml
    <!-->If you want to use IDEA or Eclipse to run code, please delete below content<-->
    <build>
        //....
    </build>
```

Then you can run the target java file.

**Example**. Suppose you want to run ```SyntheticDatasetExperiment.java```,then you will see the code in the main function.

```java
public class SyntheticDatasetExperiment {
    //....
    public static void main(String[] args){
        
        SyntheticDatasetExperiment.printFlag = true;
        SyntheticDatasetExperiment e = new SyntheticDatasetExperiment();
        // create table and add constraints
        e.initial();

        String[] filenames = {"synthetic_2M.csv", "synthetic_6M.csv", "synthetic_10M.csv"};
        String filename = filenames[0];
        System.out.println("dataset name: " + filename);
        String dir = System.getProperty("user.dir");
        String filePrefix = dir + File.separator + "src" + File.separator + "main" + File.separator;
        String filePath = filePrefix + "dataset" + File.separator + filename;

        // query json file
        String jsonFilePath = filePrefix + "java" + File.separator + "Query" + File.separator + "synthetic_query.json";
        String jsonStr = JsonReader.getJson(jsonFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonStr);

        // joinMethod = 1 -> Order Join
        // joinMethod = 2 -> Greedy Join
        int joinMethod = 1;

        // You can change function name to test different methods
        // 1.testFullScan (baseline -> VLDB'23 High Performance Row Pattern Recognition Using Joins)
        // 2.testFast (ours -> FAST for CER)
        // 3.testIntervalScan (advanced -> SIGMOD'21 Index-accelerated Pattern Matching in event stores)
        // 4.testNaiveIndexUsingRTree (naive1) or testNaiveRTreePlus (it means using interval filtering algorithm)
        // 5.testNaiveIndexUsingBPlusTree (naive2) or testNaiveBPlusTreePlus (it means using interval filtering algorithm)
        // 6.testNaiveIndexUsingSkipList (naive3) or testNaiveSkipListPlus (it means using interval filtering algorithm)

        e.testFast(filePath, jsonArray, joinMethod);
    }
}
```

The code first creates event schema, index structures, and constraint property value ranges based on the statement.

Next, it loads the record file. 

Then it loads the query statement json file. 

Finally, it executes a specific method for querying.



## Section 2 Program Code

### Section 2.1 Package

| Name         | Explanation                                       |
| ------------ | ------------------------------------------------- |
| ArfsConfig   | Arrivals parameters                               |
| BPlusTree    | BPlusTree structure                               |
| Common       | Commonly used public classes                      |
| Condition    | Predicate constraints                             |
| Experiment   | **Experimental evaluation**                       |
| FastModule   | FAST structure module, including                  |
| Generator    | Data Autogenerator                                |
| JoinStrategy | Join algorithm for bucket joining                 |
| Method       | **Index/FullScan Method for CER**                 |
| Query        | Query statement, all queryies stored in json file |
| RTree        | Rtree structure                                   |
| SkipList     | Skiplist structure we implemented                 |
| Store        | Store records in a file                           |

### Section 2.2 Key Class 

| Package      | Name                       | Explanation                                  |
| ------------ | -------------------------- | -------------------------------------------- |
| Experiment   | SyntheticDatasetExperiment | Main class for running synthesis experiments |
| Method       | FASTIndex                  | Our FAST index structure for CER             |
| JoinStrategy | OrderJoin                  | order join for generating matched tuple      |

In our paper, *skip-till-any-match* strategy is referred to as S1, but in the code, we use S3 to refer to it, in order to follow the usual usage of other papers.


## Section 3 Base-2 Bit Sliced Range Encoded Bitmap (Range Bitmap)

blog url: https://richardstartin.github.io/posts/range-bitmap-index#implementing-a-range-index

Chee Yong Chan, Yannis E. Ioannidis. Bitmap Index Design and Evaluation. SIGMOD. 1998, p355-366.

## Section 4 Datasets

Our paper used both synthetic and real datasets.

We provided the download URLs for the real dataset.

You can also choose to send emails to the author to inquire about the real datasets.

We will be very happy to receive your emails and will respond promptly.

### Section 4.1  Synthetic dataset

We have written a synthetic data generator to automatically generate synthetic data of a specified size.

Generator folder has SyntheticQueryGenerator.java file can generate synthetic dataset.

Some details: 

event schema:```String type, int attribute1, int attribute2, double.2 attribute3, double.2 attribute4, long timestamp```

```
zipf alpha= 1.6
attribute1 ~ U[0,1000]
attribute2 ~ N(500,150) and 0<= attribute2 <= 1000
attribute3 ~ U[0,1000]
attribute4 ~ N(500,150) and 0<= attribute4 <= 1000
```

Suppose the probability of occurrence of event types follows a Zipf distribution

The difference in timestamp for each adjacent record is 1.

We generated 3 synthetic datasets, they contain 2_000_000 (2M), 6_000_000 (6M), and 10_000_000 (10M) records, respectively.

### Section 4.2 Real dataset1: Stock Market

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/

event schema:```stock name, transaction ID, volume, price, time, type```

index attribute: stock name, volume, price (when using tree index, we need to index type)

number of records: 224_473 (0.224M)

number of types: 2 (Sell or Buy)

### Section 4.3  Real dataset2: Google cluster

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/
we choose four attributes from job table: event type,job ID,scheduling class,time
index attribute: scheduling class (when using tree index, we need to index type)

### Section 4.4 Real dataset3: Crimes

download URL: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g

we choose 7 columns ```Primary Type (String),ID (int),Beat (int), District(int), Latitude (Double.9), Longitude (Double.9), Date (Date format)```

we transform the date to timestamp, and sort the record according to timestamp

index attribute: Beat, District, Latitude, Longitude (when using tree index, we need to index type)

number of records: 76438 (0.224M)