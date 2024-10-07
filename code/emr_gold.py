from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

# Create a Spark session with the necessary configurations
spark = SparkSession.builder \
    .appName("IcebergGoldZoneMerge") \
    .config("spark.sql.defaultCatalog", "demo_db") \
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXXXX/") \  # change this
    .config("spark.sql.catalog.demo_db.type", "glue") \
    .config("spark.sql.catalog.demo_db.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo_db.glue.skip-name-validation", "false") \
    .getOrCreate()

# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")


# Define a function to process gold zone transformations with merge operation
def process_gold_zone_with_merge(source_table: str, gold_table: str):
    # Check if the source table exists
    if spark.catalog.tableExists(source_table):
        # Read data from the source table
        rides_df = spark.read.table(source_table)

        # Perform aggregations and calculations
        aggregated_rides = rides_df.groupBy("driver_number").agg(
            avg("max_speed").alias("avg_speed"),
            sum("distance_travelled").alias("total_distance"),
            avg("fare_amount").alias("avg_fare"),
            sum("fare_amount").alias("total_fare")
        )

        aggregated_rides.createOrReplaceTempView("aggregated_rides_view")

        # Check if the gold table exists for merge operation
        if spark.catalog.tableExists(gold_table):
            print(f"Found {gold_table}, merging the data..!!")

            # Merge operation to update existing records and insert new ones
            merge_query = f"""
            MERGE INTO {gold_table} AS target
            USING aggregated_rides_view AS source
            ON target.driver_number = source.driver_number
            WHEN MATCHED THEN 
              UPDATE SET 
                target.avg_speed = source.avg_speed,
                target.total_distance = source.total_distance,
                target.avg_fare = source.avg_fare,
                target.total_fare = source.total_fare
            WHEN NOT MATCHED THEN 
              INSERT (driver_number, avg_speed, total_distance, avg_fare, total_fare)
              VALUES (source.driver_number, source.avg_speed, source.total_distance, source.avg_fare, source.total_fare)
            """

            # Execute the merge query
            spark.sql(merge_query)
        else:
            print(f"Not Found {gold_table}, creating the table and inserting data..!!")
            aggregated_rides.writeTo(gold_table).create()

    else:
        print(f"{source_table} table is not present in glue..!")


if __name__ == "__main__":
    # Process the gold zone table with merge logic
    process_gold_zone_with_merge(source_table="demo_db.uber_database.rides_silver_data",
                                 gold_table="demo_db.uber_database.rides_gold_data")

    # Stop the Spark session
    spark.stop()