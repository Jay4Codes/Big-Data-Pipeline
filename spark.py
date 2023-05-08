from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

print('Starting spark Session')
spark.range(100).write.mode("overwrite").parquet(
    "gs://blissful-hash-312105/random_numbers")

# Load the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./site_data.csv")

# Print the schema of the DataFrame
df.printSchema()

# Show the first 20 rows of the DataFrame
df.show(20)

print('complete')

# Stop the SparkSession
spark.stop()
