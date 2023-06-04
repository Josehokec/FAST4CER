# An index Framework for Accelerating Sequential paTtern recognition (FAST)

## About running

### Run Method1: use shell script

We have written a shell script ```run.sh``` to let the program run automatically.

You can run the program using the command:

```
./run.sh start test.log
```

If you want to track the output in real-time, you can enter the following command:

```
tail -f test.log
```

#### Result explainations

```
14-th query start...                      // 14-th query pattern
filter cost: 8.602ms											// index filtering cost
scan cost: 1.782ms	                      // disk accessing cost
bucket sizes: [ 4 1441 39 ]								// each bucket size in bucket joining stage
join cost: 0.374ms                        // bucket joining cost
number of tuples: 3                       // number of matched tuples
14-th query pattern time cost: 11.13ms    // sum cost
```



### Run Method2: use IDEA or Esclipes

You can open ```FAST``` folder with IDEA or Esclipes. Notice that you need to delete part of pom.xml code lines to make it work.

```xml
    <!-->If you want to use IDEA or Eclipse to run code, please delete below content<-->
    <build>
        //....
    </build>
```

Then you can run the target java file.




## About Base-2 Bit Sliced Range Encoded Bitmap

blog url: https://richardstartin.github.io/posts/range-bitmap-index#implementing-a-range-index

Chee Yong Chan, Yannis E. Ioannidis. Bitmap Index Design and Evaluation. SIGMOD. 1998, p355-366.

## About Datasets

Our paper used both synthetic and real datasets.

We provided the download URLs for the real dataset.

You can also choose to send an email to the author to inquire about the real datasets.

We will be very happy to receive your emails and will respond promptly.

#### Synthetic dataset

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



#### Real dataset1: Stock Market

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/

event schema:```stock name, transaction ID, volume, price, time, type```

index attribute: stock name, volume, price (when using tree index, we need to index type)

number of records: 224_473 (0.224M)

number of types: 2 (Sell or Buy)



### Real dataset2: Google cluster

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/
we choose four attributes from job table: event type,job ID,scheduling class,time
index attribute: scheduling class (when using tree index, we need to index type)



#### Real dataset3: Crimes

download URL: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g

we choose 7 columns ```Primary Type (String),ID (int),Beat (int), District(int), Latitude (Double.9), Longitude (Double.9), Date (Date format)```

we transform the date to timestamp, and sort the record according to timestamp

index attribute: Beat, District, Latitude, Longitude (when using tree index, we need to index type)

number of records: 76438 (0.224M)