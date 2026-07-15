from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, mean, stddev, format_number, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
spark = SparkSession.builder \
    .appName("S3JsonReader") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()


def clean_dataframe(df):
    """
    1. Trims and normalizes all column headers.
    2. Trims leading/trailing whitespace from text data fields.
    """
    # Step 1: Clean column names (strip space, replace spaces with underscores, lowercase)
    cleaned_col_names = [
        F.col(f"`{c}`").alias(c.strip().lower().replace(" ", "_")) 
        for c in df.columns
    ]
    df_header_clean = df.select(cleaned_col_names)
    
    # Step 2: Clean text cell content (only touch StringType columns)
    df_fully_clean = df_header_clean.select([
        F.trim(F.col(c)).alias(c) if isinstance(datatype, StringType) or datatype == "string" else F.col(c)
        for c, datatype in df_header_clean.dtypes
    ])
    
    return df_fully_clean




#read first df 
pr_df = spark.read \
    .option("delimiter", "\\t") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://shvnsh-rearc-quest/data/bls_data/pr.data.0.Current")
# View the metadata and raw content column
pr_cleaned_df = clean_dataframe(pr_df)
for c, datatype in pr_cleaned_df.dtypes:
    print(f"Column: {c}, Data Type: {datatype}")
    print(isinstance(datatype, StringType))
pr_cleaned_df.select("series_id","period").distinct().show(1000000, truncate=False)

pr_cleaned_df.printSchema()


filtered_ts = pr_cleaned_df.filter(
    (F.col("series_id") == "PRS30006032") & 
    (F.col("period") == "Q01")
)

filtered_ts.show(1000,truncate=False)

yearly_sum_df = pr_cleaned_df.groupBy("series_id", "year") \
    .agg(F.sum("value").alias("total_value"))

window_spec = Window.partitionBy("series_id").orderBy(F.col("total_value").desc())

report_df = yearly_sum_df.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .select("series_id", "year", F.col("total_value").alias("value"))

report_df.show()



s3_path = "s3a://shvnsh-rearc-quest/data/datausa/population_data.json"
df = spark.read.json(s3_path)
exploded_df = df.select(explode(col("data")).alias("record"))

# 4. Flatten the struct. The 'record.*' command extracts the dictionary keys into actual DataFrame columns.
final_df = exploded_df.select("record.*")

# Optional: Cast Population to Integer to remove the .0 decimal
final_df = final_df.withColumn("Population", col("Population").cast("integer"))

population_df = clean_dataframe(final_df)

filtered_population_df = population_df.filter(col("Year").between(2013, 2018))

# 2. Aggregate to find the mean and standard deviation
stats_df = filtered_population_df.agg(
    mean("population").alias("Mean_Population"),
    stddev("population").alias("StdDev_Population")
)

# 3. Optional: Format the numbers to make them readable (adding commas, rounding to 2 decimal places)
formatted_stats_df = stats_df.select(
    format_number(col("Mean_Population"), 2).alias("Mean_Population"),
    format_number(col("StdDev_Population"), 2).alias("StdDev_Population")
)

# View the results
formatted_stats_df.show()


filtered_ts = pr_cleaned_df.filter(
    (F.col("series_id") == "PRS30006032") & 
    (F.col("period") == "Q01")
)

filtered_ts.show(1000,truncate=False)

# Step 2: Left join with Part 2 population dataset matching on 'year'
report_df = filtered_ts.join(
    population_df,
    on="year", 
    how="inner"
)

report_df.show(truncate=False)