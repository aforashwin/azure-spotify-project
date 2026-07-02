# Databricks notebook source
# MAGIC %md
# MAGIC ## Autoloader 

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimUser

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimUser/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimUser")



# COMMAND ----------

df_preview = spark.read.format("parquet") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimUser")

display(df_preview.limit(5))

# COMMAND ----------

# DBTITLE 1,h
df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

df_user = df_user.dropDuplicates()

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimUser/data")\
    .toTable("dbx_spotify_cat.silver.DimUser")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbx_spotify_cat.silver.dimuser limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimArtist

# COMMAND ----------

df_artist = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_art_pre = spark.read.format("parquet")\
                  .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimArtist")
display(df_art_pre.limit(5))

# COMMAND ----------

df_artist = df_artist.dropDuplicates()\
                     .withColumn("artist_name",upper(col("artist_name")))\
                     .withColumn("artist_name",lower(col("country")))
                     

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimArtist/data")\
    .toTable("dbx_spotify_cat.silver.DimArtist")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbx_spotify_cat.silver.dimartist limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimDate/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimDate")


# COMMAND ----------

df_date_pre = spark.read.format("parquet")\
                        .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimDate")
display(df_date_pre.limit(5))

# COMMAND ----------

df_date.dropDuplicates()

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimDate/data")\
    .toTable("dbx_spotify_cat.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimTrack/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimTrack")


# COMMAND ----------

df_track_pre = spark.read.format("parquet")\
                    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimTrack")
display(df_track_pre.limit(5))

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimTrack/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track_pre = spark.read.format("parquet")\
                         .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimTrack")
display(df_track_pre.limit(5))

# COMMAND ----------

df_track= df_track.select(
    col("track_id"),
    upper(col("track_name")).alias("track_name"),   # convert to uppercase
    col("artist_id"),
    col("album_name"),
    col("duration_sec"),
    col("release_date"),

    # duration category logic
    when(col("duration_sec") >= 200, "HIGH")
    .when((col("duration_sec") >= 120) & (col("duration_sec") < 200), "MEDIUM")
    .otherwise("LOW")
    .alias("duration_category"),

    current_timestamp().alias("updated_at")
)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@dbxspotifylake.dfs.core.windows.net/DimTrack/data")\
    .toTable("dbx_spotify_cat.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ##FactStream

# COMMAND ----------

df_FactStream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@dbxspotifylake.dfs.core.windows.net/FactStream/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fs = spark.read.format("parquet")\
             .load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/FactStream")
display(df_fs.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, when

df_FactStream = df_FactStream.select(
    col("stream_id"),
    col("user_id"),
    col("track_id"),
    col("date_key"),
    col("listen_duration"),
    col("device_type"),

    when(col("listen_duration") >= 200, "HIGH")
        .when((col("listen_duration") >= 60) & (col("listen_duration") < 200), "MEDIUM")
        .otherwise("LOW")
        .alias("listen_category"),

    col("stream_timestamp")
)

# COMMAND ----------

df_FactStream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "abfss://silver@dbxspotifylake.dfs.core.windows.net/FactStream/checkpoint"
    ) \
    .option(
        "path",
        "abfss://silver@dbxspotifylake.dfs.core.windows.net/FactStream/data"
    ) \
    .trigger(once=True) \
    .toTable("dbx_spotify_cat.silver.FactStream")

# COMMAND ----------

df_sample = spark.read.format("parquet").load("abfss://bronze@dbxspotifylake.dfs.core.windows.net/DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC when we have specified managed location in databricks console!
# MAGIC when we use to_table() data are not stored in adls as this stores only table
# MAGIC when we use df.write as below cammand the data are store in adls layer that specified managed later location

# COMMAND ----------

df_sample.write.format("delta").saveAsTable("dbx_spotify_cat.diamound.Dimsample")

# COMMAND ----------

