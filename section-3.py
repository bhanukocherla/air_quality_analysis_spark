from pyspark.sql import SparkSession

# Start Spark Session
spark = SparkSession.builder.appName("AirQuality_SQL_Analysis").getOrCreate()

# Read the Section 2 enhanced output (whole folder, not specific file)
df = spark.read.option("header", "true").option("inferSchema", "true").csv("Output/Section2_enhanced_raw/")

# Show first few rows
df.show(5)

# Save the final output
df.write.mode("overwrite").option("header", "true").csv("Output/output_section3_csv")
