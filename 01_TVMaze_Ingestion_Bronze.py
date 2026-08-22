# Databricks notebook source
import requests
import json


url = "https://api.tvmaze.com/shows"

response = requests.get(url, timeout=60)

#print("HTTP Status:", response.status_code)

response.raise_for_status()

shows_data = response.json()



# COMMAND ----------


json_lines = "\n".join(json.dumps(record, ensure_ascii=False) for record in shows_data)


raw_shows_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/shows/tvmaze_shows.json"


dbutils.fs.put(raw_shows_path,json_lines,overwrite=True)


# COMMAND ----------

raw_shows_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/shows/tvmaze_shows.json"
shows_df = spark.read.json(raw_shows_path)

# COMMAND ----------

bronze_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/bronze/shows"

 ##Adding ingestion timestamp
from pyspark.sql.functions import current_timestamp

bronze_df = shows_df.withColumn("ingestion_timestamp",current_timestamp())


bronze_df.write.format("delta").mode("overwrite").save(bronze_path)


# COMMAND ----------

bronze_df_check = spark.read.format("delta").load( "abfss://raw@sttvmazede2026.dfs.core.windows.net/bronze/shows")
display(bronze_df_check.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, to_date, regexp_replace, trim

silver_df = bronze_df_check.select(
    col("id").alias("show_id"),
    col("name").alias("show_name"),
    col("type"),
    col("language"),
    col("genres"),
    col("status"),
    col("runtime"),
    col("averageRuntime").alias("average_runtime"),
    
    # Convert dates
    to_date(col("premiered")).alias("premiered_date"),
    to_date(col("ended")).alias("ended_date"),
    
    col("officialSite").alias("official_site"),
    
    # Schedule
    col("schedule.time").alias("schedule_time"),
    col("schedule.days").alias("schedule_days"),
    
    # Rating
    col("rating.average").alias("rating"),
    
    # Network
    col("network.name").alias("network_name"),
    col("network.country.name").alias("country_name"),
    col("network.country.code").alias("country_code"),
    
    # Web channel
    col("webChannel.name").alias("web_channel_name"),
    
    # Remove HTML tags from summary
    trim(
        regexp_replace(
            col("summary"),
            "<[^>]*>",
            ""
        )
    ).alias("summary"),
    
    col("weight"),
    col("updated").alias("updated_timestamp"),
    col("ingestion_timestamp")
)


display(silver_df.limit(10))

# COMMAND ----------

silver_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/silver/shows"

silver_df.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

silver_check = spark.read.format("delta").load("abfss://raw@sttvmazede2026.dfs.core.windows.net/silver/shows")


# COMMAND ----------

from pyspark.sql.functions import (
    col,
    year,
    when,
    count,
    avg,
    round,
    desc
)

gold_df = (
    silver_check
    .withColumn(
        "premiere_year",
        year(col("premiered_date"))
    )
    .withColumn(
        "runtime_category",
        when(col("runtime").isNull(), "Unknown")
        .when(col("runtime") <= 30, "Short")
        .when(col("runtime") <= 60, "Standard")
        .otherwise("Long")
    )
    .withColumn(
        "rating_category",
        when(col("rating").isNull(), "Not Rated")
        .when(col("rating") >= 8, "Excellent")
        .when(col("rating") >= 6, "Good")
        .otherwise("Average")
    )
)
display(gold_df)

# COMMAND ----------

gold_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/gold/shows"

gold_df.write.format("delta").mode("overwrite").save(gold_path)


# COMMAND ----------

gold_check = spark.read.format("delta").load(
    "abfss://raw@sttvmazede2026.dfs.core.windows.net/gold/shows")
display(gold_check)    

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tvmaze.default.gold_shows
USING DELTA
LOCATION 'abfss://raw@sttvmazede2026.dfs.core.windows.net/gold/shows'
""")

# COMMAND ----------



spark.sql("""
CREATE TABLE IF NOT EXISTS tvmaze.default.bronze_shows
USING DELTA
LOCATION 'abfss://raw@sttvmazede2026.dfs.core.windows.net/bronze/shows'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tvmaze.default.silver_shows
USING DELTA
LOCATION 'abfss://raw@sttvmazede2026.dfs.core.windows.net/silver/shows'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tvmaze.default.bronze_shows;
# MAGIC select count(*) from tvmaze.default.silver_shows;
# MAGIC select count(*) from tvmaze.default.gold_shows;
# MAGIC

# COMMAND ----------

# Cell 20 - Create fact_show_data

from pyspark.sql.functions import col, year

fact_show_data = (
    silver_check
    .select(
        col("show_id"),
        col("show_name"),
        col("type"),
        col("language"),
        col("status"),
        col("rating"),
        col("runtime"),
        col("average_runtime"),
        col("network_name"),
        col("country_name"),
        col("country_code"),
        col("premiered_date"),
        col("ended_date"),
        col("official_site"),
        col("web_channel_name"),
        col("weight")
    )
    .withColumn(
        "premiere_year",
        year(col("premiered_date"))
    )
)

print("fact_show_data created successfully.")
print("Record count:", fact_show_data.count())

display(fact_show_data.limit(20))

# COMMAND ----------

# Cell 21 - Save fact_show_data as Delta

fact_path = "abfss://raw@sttvmazede2026.dfs.core.windows.net/gold/fact_show_data"

fact_show_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save(fact_path)

print("fact_show_data Delta table created successfully.")
print("Location:")
print(fact_path)

# COMMAND ----------

# Cell 22 - Register fact_show_data in Unity Catalog

spark.sql("""
CREATE TABLE IF NOT EXISTS tvmaze.default.fact_show_data
USING DELTA
LOCATION 'abfss://raw@sttvmazede2026.dfs.core.windows.net/gold/fact_show_data'
""")

print("tvmaze.default.fact_show_data registered successfully.")

# COMMAND ----------

# Cell 23 - Verify fact_show_data

print(
    "fact_show_data records:",
    spark.table("tvmaze.default.fact_show_data").count()
)

display(
    spark.sql("""
        SELECT
            show_id,
            show_name,
            status,
            rating,
            runtime,
            network_name,
            country_name,
            premiere_year
        FROM tvmaze.default.fact_show_data
        LIMIT 20
    """)
)

# COMMAND ----------

# Cell 24 - Gold analytics

display(
    spark.sql("""
        SELECT
            status,
            COUNT(*) AS show_count,
            ROUND(AVG(rating), 2) AS average_rating,
            ROUND(AVG(runtime), 2) AS average_runtime
        FROM tvmaze.default.fact_show_data
        GROUP BY status
        ORDER BY show_count DESC
    """)
)

# COMMAND ----------

# Cell 25 - Top 20 rated shows

display(
    spark.sql("""
        SELECT
            show_id,
            show_name,
            rating,
            status,
            runtime,
            country_name
        FROM tvmaze.default.fact_show_data
        WHERE rating IS NOT NULL
        ORDER BY rating DESC
        LIMIT 20
    """)
)

# COMMAND ----------

# Cell 26 - Shows by country

display(
    spark.sql("""
        SELECT
            country_name,
            COUNT(*) AS show_count,
            ROUND(AVG(rating), 2) AS average_rating
        FROM tvmaze.default.fact_show_data
        WHERE country_name IS NOT NULL
        GROUP BY country_name
        ORDER BY show_count DESC
    """)
)

# COMMAND ----------

# Cell 27 - Shows by premiere year

display(
    spark.sql("""
        SELECT
            premiere_year,
            COUNT(*) AS show_count,
            ROUND(AVG(rating), 2) AS average_rating
        FROM tvmaze.default.fact_show_data
        WHERE premiere_year IS NOT NULL
        GROUP BY premiere_year
        ORDER BY premiere_year
    """)
)

# COMMAND ----------

# Cell 28 - Optimize Gold table

spark.sql("""
OPTIMIZE tvmaze.default.gold_shows
""")

print("Gold table optimized successfully.")

# COMMAND ----------

# Cell 29 - Optimize fact_show_data

spark.sql("""
OPTIMIZE tvmaze.default.fact_show_data
""")

print("fact_show_data optimized successfully.")

# COMMAND ----------

# Cell 30 - Z-Order fact_show_data

spark.sql("""
OPTIMIZE tvmaze.default.fact_show_data
ZORDER BY (country_name, status, rating)
""")

print("fact_show_data Z-Order optimization completed.")

# COMMAND ----------


