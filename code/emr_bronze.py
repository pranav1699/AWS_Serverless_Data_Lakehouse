from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# Create a Spark session with the necessary configurations
spark = SparkSession.builder \
    .appName("IcebergWithAWS") \
    .config("spark.sql.defaultCatalog", "demo_db") \
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXXX/") \  # change this
    .config("spark.sql.catalog.demo_db.type", "glue") \
    .config("spark.sql.catalog.demo_db.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo_db.glue.skip-name-validation", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def read_data() -> (DataFrame, int):
    rides = spark.read.csv("s3://XXXXXXX/data/*.csv", header=True, inferSchema=True)
    rides_count = rides.count()
    if spark.catalog.tableExists("demo_db.uber_database.rides"):
        checkpoint_time = lookup_activity(table="demo_db.uber.rides", column="Date")
        new_rides = rides.where(col("Date") > checkpoint_time)
        new_rides_count = new_rides.count()
        return new_rides, new_rides_count
    else:
        return rides, rides_count


def lookup_activity(table: str, column: str) -> str:
    table = spark.table(table)
    checkpoint_time = table.selectExpr(f"max({column})").collect()[0][0]
    return checkpoint_time


def load_data(rides: DataFrame) -> None:
    if spark.catalog.tableExists("demo_db.uber_database.rides"):
        print("APPENDING THE DATA...!!!")
        (
            rides
            .writeTo("demo_db.uber_database.rides")
            .append()
        )
    else:
        print("CREATING THE TABLE ....!!!")
        (
            rides
            .writeTo("demo_db.uber_database.rides")
            .create()
        )


if __name__ == "__main__":
    rides_data, rides_data_count = read_data()
    if rides_data_count > 0:
        load_data(rides_data)
    else:
        print("NO NEW RIDES WERE ADDED...!!!")

        # Stop the Spark session
    spark.stop()