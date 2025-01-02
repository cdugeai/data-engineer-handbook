#!/usr/bin/env python
# coding: utf-8

# ## SPARK homework

# In[13]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, split, lit, avg, sum, desc, count, col

spark = (
    SparkSession
    .builder
    .appName("Jupyter")
    # Q1. Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark


# In[2]:


matches = spark.read.csv("/home/iceberg/data/matches.csv", header=True, inferSchema=True)
match_details = spark.read.csv("/home/iceberg/data/match_details.csv", header=True, inferSchema=True)
medals_matches_players = spark.read.csv("/home/iceberg/data/medals_matches_players.csv", header=True, inferSchema=True)
medals = spark.read.csv("/home/iceberg/data/medals.csv", header=True, inferSchema=True)
maps = spark.read.csv("/home/iceberg/data/maps.csv", header=True, inferSchema=True)


# In[14]:


# Q2. Explicitly broadcast JOINs `medals` and `maps`
#   and rename some columns for later analysis

medals_broadcast = broadcast(medals.withColumnRenamed("name", "medalname"))
maps_broadcast = broadcast(maps.withColumnRenamed("name", "mapname"))


# In[5]:


# Create the 4 bucketed tables

spark.sql("DROP TABLE IF EXISTS bootcamp.match_details_bucketed")
match_details.write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.match_details_bucketed")
spark.sql("DROP TABLE IF EXISTS bootcamp.matches_bucketed")
matches.write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.matches_bucketed")
spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed")
medals_matches_players.write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.medals_matches_players_bucketed")


# In[6]:


# Loading the bucketed tables

match_details_bucketed = spark.read.table("bootcamp.match_details_bucketed")
matches_bucketed = spark.read.table("bootcamp.matches_bucketed")
medals_matches_players_bucketed = spark.read.table("bootcamp.medals_matches_players_bucketed")


# In[7]:


# Q3. Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
#   with the broadcast data too
analytical_df = match_details_bucketed \
    .join(matches_bucketed, "match_id", "inner") \
    .join(medals_matches_players_bucketed, ["match_id", "player_gamertag"], "inner") \
    .join(medals_broadcast, "medal_id", "inner") \
    .join(maps_broadcast, "mapid", "inner")


# In[8]:


# Q4.1. Player most kills

analytical_df \
    .groupBy("player_gamertag") \
    .agg(sum("player_total_kills").alias("metric_player_total_kills")) \
    .orderBy(desc("metric_player_total_kills")) \
    .head(3)


# In[9]:


# Q4.2. Playlist most played

analytical_df \
    .groupBy("playlist_id") \
    .agg(count("match_id").alias("metric_playlist_plays")) \
    .orderBy(desc("metric_playlist_plays")) \
    .head(3)


# In[10]:


# Q4.3. Map most played

analytical_df \
    .groupBy("mapname") \
    .agg(count("match_id").alias("metric_map_plays")) \
    .orderBy(desc("metric_map_plays")) \
    .head(3)


# In[11]:


# Q4.4. Most common maps played when medal "Killing Spree" is earned

analytical_df \
    .filter(col("medalname")=="Killing Spree") \
    .groupBy("medalname", "mapname") \
    .agg(count("mapname").alias("metric_map_plays")) \
    .orderBy(desc("metric_map_plays")) \
    .head(3)


# In[12]:


# Q5. Get size when repartitionning of different columns

for partition_column in ["playlist_id", "mapid", "match_id", "player_gamertag"]:
    (
        analytical_df
        .repartition(4, partition_column)
        .sortWithinPartitions(partition_column)
        # Select only some cols beacause on join, several columns have same name
        .select("medalname", "mapid", "mapname", "playlist_id", "match_id", "player_gamertag")
        .write
        .mode("overwrite")
        .saveAsTable("bootcamp.analytical_df_partitionned_test")
    )
    # spark.sql("Select * FROM demo.bootcamp.analytical_df_partitionned_test limit 5").show()
    spark.sql("DROP TABLE IF EXISTS demo.bootcamp.analytical_df_partitionned_test.files")
    spark.sql("SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, '"+partition_column+"' as partitioned_by FROM demo.bootcamp.analytical_df_partitionned_test.files").show()

print("Size looks smaller with partition on match_id")

