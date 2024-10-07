from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session with the necessary configurations
spark = SparkSession.builder \
    .appName("IcebergWithAWS") \
    .config("spark.sql.defaultCatalog", "demo_db") \
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXX/") \  # change this
    .config("spark.sql.catalog.demo_db.type", "glue") \
    .config("spark.sql.catalog.demo_db.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo_db.glue.skip-name-validation", "false") \
    .getOrCreate()

# Set the log level to ERROR to reduce verbosity in logs
spark.sparkContext.setLogLevel("ERROR")


def lookup_activity(table: str, column: str) -> str:
    # Read the table and select the maximum value of the specified column
    table_df = spark.table(table)
    checkpoint_time = table_df.selectExpr(f"max({column})").collect()[0][0]
    return checkpoint_time


def read_and_transform(table_source: str, table_dest: str, drop_columns: list, rename_columns: dict):
    if spark.catalog.tableExists(table_source):
        # Read data from the source table
        rides_df = spark.read.table(table_source)
        silver_rides = rides_df.drop(*drop_columns).withColumnRenamed("speed", "max_speed")
        silver_rides.printSchema()
        # Check if the destination table exists
        if spark.catalog.tableExists(table_dest):
            # Get the latest timestamp from the destination table
            checkpoint_time = lookup_activity(table=table_dest, column="Date")
            # Filter new data based on the latest timestamp
            new_rides = silver_rides.where(col("Date") > checkpoint_time)
            # If there is new data, process and write it to the destination table
            if new_rides.count() > 0:

                print(f"Found {table_dest} appending the data..!!")

                # Append the new data to the destination table
                (
                    new_rides
                    .writeTo(table_dest)
                    .partitionedBy(col("Date"))
                    .append()
                )
            else:
                print("no new rows were added..!")
        else:
            # If the destination table does not exist, create it
            print(f"Not Found {table_dest} creating the table..!!")
            (
                silver_rides
                .writeTo(table_dest)
                .partitionedBy(col("Date"))
                .create()
            )
    else:
        # If the source table does not exist, print an error message
        print(f"{table_source} table is not present in glue..!")


if __name__ == "__main__":
    # Run the read and transform function with specified parameters
    read_and_transform(table_source="demo_db.uber_database.rides", table_dest="demo_db.uber_database.rides_silver_data",
                       drop_columns=["route_taken", "start_point", "end_point"], rename_columns={"speed": "max_speed"})

    # Stop the Spark session
    spark.stop()