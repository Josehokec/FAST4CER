# An index Framework for Accelerating Sequential paTtern recognition (FAST)

## About running
### Run Method1: shell script
We have written a shell script ```run.sh``` to let the program run automatically

You can run the program using the command:
```
./run.sh start output.log
```

If you want to track the output in real-time, you can enter the following command:

```
tail -f test.log
```

### Run Method2: IDEA or Esclipes 

You can open ```FAST``` folder with IDEA or Esclipes.

Notice that you need to delete part of pom.xml code to make it work.

```
<!-->If you want to use IDEA or Eclipse to run code, please delete below content<-->
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <executions>
                    <execution>
                        <id>copy-dependencies</id>
                        <phase>package</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/lib</outputDirectory>
                            <overWriteReleases>false</overWriteReleases>
                            <overWriteSnapshots>false</overWriteSnapshots>
                            <overWriteIfNewer>true</overWriteIfNewer>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifestEntries>
                            <Class-Path>.</Class-Path>
                        </manifestEntries>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```




## About Base-2 Bit Sliced Range Encoded Bitmap
blog url: https://richardstartin.github.io/posts/range-bitmap-index#implementing-a-range-index

Chee Yong Chan, Yannis E. Ioannidis. Bitmap Index Design and Evaluation. SIGMOD. 1998, p355-366.

## About Dataset
#### synthetic dataset

Generator folder has SyntheticQueryGenerator.java file can generate synthetic dataset

Details:

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

We generate 5 synthetic datasets, they contain 2_000_000 (2M), 4_000_000 (4M), 
8_000_000 (8M), 16_000_000 (16M), 32_000_000 (32M) records, respectively.

#### real dataset1: the stock market dataset

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/

event schema:```stock name, transaction ID, volume, price, time, type```

index attribute: stock name, volume, price (when using tree index, we need to index type)

number of records: 224_473 (0.224M)

number of types: 2 (Sell or Buy)

### real dataset2: Google cluster

download URL: https://davis.wpi.edu/datasets/Stock_Trace_Data/stock_data/
we choose four attributes from job table: event type,job ID,scheduling class,time
index attribute: scheduling class (when using tree index, we need to index type)
#### real dataset3: crimes dataset

download URL: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g

we choose 7 columns ```Primary Type (String),ID (int),Beat (int), District(int), Latitude (Double.9), Longitude (Double.9), Date (Date format)```

we transform the date to timestamp, and sort the record according to timestamp

index attribute: Beat, District, Latitude, Longitude (when using tree index, we need to index type)

number of records: 76438 (0.224M)
