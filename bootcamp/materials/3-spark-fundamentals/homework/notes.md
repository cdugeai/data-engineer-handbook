Spark: distributed computing framework to compute high volume of data
Hadoop (2009) -> Java MapReduce -> Hive -> Spark

Spark is storage agnostic, no vendor lock

NOt good: if only person in the company using Spark.

How Spark work: (ex: Basketball team)
- the plan (the plays)
- driver (coach)
- executors (players)


The plan: **transformation** described in Python, Scala, SQL, or R: **lazy evaluation**
The Driver: reads the plan. Important driver settings `spark.driver.memory` and `spark.driver.memoryOverheadFactor` to add memory to the JVM (not for the jobs). Shouldnt touch other driver settings.

Driver determines:
- when to stop beeing lazy
- how to join datasets
- how much parralelism needed in each step

Executors:
- the driver passes the plan to the executors
- `spark.executor.memory`: memory for jobs. If too small -> Spark will *spill to disk*
- `spark.executor.cores`: how many jobs can run on one machine, default 4
- `spark.executor.memoryOverheadFactor`

## Types of JOINs in Spark

Shuffle sort-merge JOIN:
- default strategy
- works when both sides are large
- try to avoid and use the followings

Broadcast hash JOIN:
- one side is small
- will ship the **entire** small dataset to the executor -> this is a JOIN **without shuffle** !
- `spark.sql.autoBroadcastJoinThreshold`

Bucket join: 
- this is a JOIN **without shuffle** ! Using buckets instead

## How shuffle works ?

Default Spark n° of partitions: 200
Least scalable part.

## Shuffle

Shuffle partitions and parallelism are linked. Use `spark.sql.shuffle.partitions` (same as `spark.default.parallelism`, related to RDD API)
Shouldnt use RDD API, but igher level API: Dataframes SparkSQL Dataset API. 
Minimize shuffle techniques:
- bucket data -> always use **powers of 2 of buckets**

Skew: 
- some partitions may have + data than others (ex: all notifications from The Rock + Beyonce go to the same partitions so same executor).
- symptom: job fail at 99% or do box and whisper distribution to find outliers
- solution: 
    - `spark.sql.adaptative.enabled = True`: adaptative query execution (Spark 3+)
    - OR salt the GROUP BY: add random column and add it to the GROUP BY

Spark query plans: `df.explain()` will show the join strategies

Spark read data from:
- lake: delta lake, Apache Iceberg, Hive metastore,
- RDBMS: Postgres, Oracle, ...
- API: REST call (done **on the driver** !). Use `spark.parralelise(API_call)` to make the call in executor.
- Flat file: CSV, JSON

Spark output datasets should be **partitioned on date** (date of the pipeline run). Is commonly called *ds partitionning*.

## Lab 1

Use `.sortWithPartitions()` and **NOT** global `.sort()`
In query plan, *Explain* means *shuffle*
In query plan, *Project* means *SELECT*

Iceberg: sort will shrink data size. Sort by lowest cardinality first to the highest.

## Lecture 2

Spark Server vs Notebooks.
Temporary View: like CTE. **RE-computed** for each downstream use **unless it is cached**
Caching storage level: MEMORY_ONLY, DISK_ONLY (like materailized view), MEMORY_AND_DISK (default)
If caching df to disk, should replace by staging table. Caching only to memory.
Data stays partitions when cached VS in **broadcast JOIN** where all data sent to single executor. So can cached high volume bcs partitioned.


Broadcast **prevent shuffle**
Broadcast join threshold: `sparl.sql.autoBroadcastJoinThreshold`, default 10Mo. Or use `broadcast(df)`

UDFs: User Defined Functions
Dataset API, only in Scala.

Dataframe vs Dataset vs SparkSQL
- SparkSQL: lowest barrier to entry. Good if DScintist work on this too. only need SQL.
- Dataframe: can modularize code, test code, create functions.
- Dataset: Scala only. Best for pipelines that need unit test. Can easily create mock data. Handles NULLs better.

Parquet: **run-length encoding** -> powerful compression. Don't use `.sort()` but `.sortWithinPartitions()`
Iceberg uses parquet as default file format.

Spark Tuning: 
- executor memory: don't set 16GB
- driver memory: dont bump it, unless using `df.collect()` or very complex job
- shuffle partitions: default 200. Rule of thumb: around 100 MB per partition
- Adaptative Query Execution (AQE): use with skewed datasets

## Lab 2

Dataset API Scala `user_id.getOrElse(deefault_value)` is like COALESCE
Dataset API Scala `user_id.get` gives the content of an *Option*
Cache: `df.cache()`
Use 
```scala
// Use
df.write.mode("overwrite").saveAsTable("my_schema.tablename1")
// not
df.persist(StorageLevele.DISK_ONLY)
```

Bucket joins: `PARTITIONED BY (col_1, col_2, bucket(16, col_3))`

## Lecture 2

Unit + integration tests.
Catch bug in prod: use **write-audit-publish pattern**.

SE has more quality standards than DE



