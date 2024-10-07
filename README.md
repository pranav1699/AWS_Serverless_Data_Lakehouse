### Building a Serverless Lakehouse Batch Data Pipeline with Apache Iceberg, Apache Spark on EMR Serverless, AWS Glue, and Event-Driven Orchestration Using Step Functions

Automating data pipelines with Spark and Iceberg in a serverless cloud — every new file triggers seamless data processing without a single server to manage.

![Building a Serverless Lakehouse Batch Data Pipeline](https://cdn-images-1.medium.com/max/2400/1*gWqdkCL1MS2Br2bsX7NHgw.jpeg)

In this blog, I will guide you through the steps I took to create a fully automated, serverless batch data pipeline using AWS services and open-source frameworks like Apache Spark and Apache Iceberg. This architecture processes CSV files stored in S3 and stores the transformed data into Iceberg tables, with AWS Glue for data cataloging and Step Functions for orchestration. The entire pipeline is triggered by file uploads via Amazon EventBridge.

#### Project Overview

The goal of this project was to process incoming CSV files using Apache Spark, within a **lakehouse architecture** that integrates a **medallion architecture** (organized into **bronze**, **silver**, and **gold** zones), while orchestrating everything through AWS Step Functions and EventBridge for **real-time event-driven** processing. The architecture supports **incremental data processing**, ensuring that only new or modified data is handled efficiently.

In this **lakehouse architecture**, data is stored in a centralized S3 repository with the scalability of a data lake and the performance of a data warehouse. The **medallion architecture** introduces structured layers for data refinement:

- **Bronze zone:** Stores raw, ingested data directly from CSV files.
- **Silver zone:** Houses cleaned and transformed data, prepped for further analysis.
- **Gold zone:** Contains fully optimized, analytics-ready data for reporting and insights.

The use of **real-time event-driven** processing enables the system to react to new data as soon as it arrives in S3, triggering the pipeline through EventBridge and Step Functions. This ensures low-latency data ingestion and processing. Combined with **incremental data processing**, only new or updated records are processed, further enhancing efficiency.

This solution is **serverless**, highly scalable, and **cost-effective**, providing a robust framework for both **real-time** and **batch processing** of large-scale data pipelines while maintaining high performance and data quality.

#### Architecture Components

1. **Amazon EMR Serverless**: To run Spark jobs in a fully managed, serverless way.
2. **AWS Glue**: For data cataloging and managing Iceberg tables.
3. **Amazon S3**: For storing raw data, Pyspark scripts, and processed Iceberg tables.
4. **AWS Step Functions**: To orchestrate the pipeline.
5. **Amazon EventBridge**: To trigger the pipeline automatically on file uploads.

### Step-by-Step Implementation

#### 1. Creating the Necessary IAM Roles

To ensure proper communication between AWS services, I created three IAM roles with appropriate permissions:

- **EMR Service Role**: Allows EMR Serverless to access other AWS services.
- **Step Functions Role**: Manages orchestrations with full admin access.
- **CloudWatch Events Role**: Grants access to CloudWatch logs and metrics.

Here’s an example of the _EMR Service Role_ configuration:

```json
{  
  "Version": "2012-10-17",  
  "Statement": [  
    {  
      "Effect": "Allow",  
      "Principal": {  
        "Service": "emr-serverless.amazonaws.com"  
      },  
      "Action": "sts:AssumeRole"  
    }  
  ]  
} 
```

Similarly, the Step Functions and CloudWatch roles were configured with admin permissions for simplicity, though more fine-grained permissions can be applied in production.

```json
{  
  "Version": "2012-10-17",  
  "Statement": [  
    {  
      "Effect": "Allow",  
      "Principal": {  
        "Service": "states.amazonaws.com"  
      },  
      "Action": "sts:AssumeRole"  
    }  
  ]  
}
```

```json
{  
  "Version": "2012-10-17",  
  "Statement": [  
    {  
      "Effect": "Allow",  
      "Principal": {  
        "Service": "events.amazonaws.com"  
      },  
      "Action": "sts:AssumeRole"  
    }  
  ]  
}
```

#### 2. Setting Up Amazon EMR Serverless

To run Spark jobs without provisioning any infrastructure, I created an **EMR Serverless application** with **EMR version 7.2.0**, which comes with Iceberg preinstalled. This version supports a data lakehouse architecture, removing the need for extensive configurations to work with data formats like **Apache** **Iceberg**.

![](https://cdn-images-1.medium.com/max/1600/1*essFZrnhQjgwRN09D06FCw.png)

#### 3. Creating S3 Buckets for Data and Code

I created three S3 buckets for this pipeline:

1. **Raw Data Bucket**: Stores CSV files.
2. **Code Bucket**: Stores Pyspark scripts.
3. **Processed Data Bucket**: Stores the output data in Iceberg format.

![](https://cdn-images-1.medium.com/max/1600/1*JK8dSqKUe1VXYm9k5maKdg.png)

#### 4. Creating a Glue Database and Catalog

Next, I set up **AWS Glue** to manage the metadata of my **Iceberg tables**. This is crucial for cataloging data in a structured way, making it easy to query and process.

Create a database called `uber_database` for this project

![](https://cdn-images-1.medium.com/max/1600/1*SYSEZs_1DVpiXJPaPlxgrw.png)

#### 5. Writing the Pyspark Script

The core processing logic is in the Pyspark script, which reads data from the raw CSV files, processes it, and stores the result in Iceberg tables. The pipeline consists of multiple zones — bronze, silver, and gold — where data undergoes various transformations.

**Bronze Zone**

Here, the raw CSV data is ingested with minimal transformations. Here’s the Pyspark script:
  
```python
from pyspark.sql import SparkSession, DataFrame    
from pyspark.sql.functions import col   

spark = SparkSession.builder \    
    .appName("IcebergWithAWS") \    
    .config("spark.sql.defaultCatalog", "demo_db") \    
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \    
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXXX/") \  #change this  
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
```
**Silver Zone**

In this zone, further transformations are applied, such as filtering duplicates and renaming columns:

```python
from pyspark.sql import SparkSession    
from pyspark.sql.functions import col    
    
# Create a Spark session with the necessary configurations    
spark = SparkSession.builder \    
    .appName("IcebergWithAWS") \    
    .config("spark.sql.defaultCatalog", "demo_db") \    
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \    
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXX/") \ #change this   
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
```

**Gold Zone**
In the Gold Zone, data is aggregated for analytical purposes.
```python
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, avg, sum  
  
# Create a Spark session with the necessary configurations  
spark = SparkSession.builder \  
    .appName("IcebergGoldZoneMerge") \  
    .config("spark.sql.defaultCatalog", "demo_db") \  
    .config("spark.sql.catalog.demo_db", "org.apache.iceberg.spark.SparkCatalog") \  
    .config("spark.sql.catalog.demo_db.warehouse", "s3://XXXXXXX/") \ #change this  
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
```
upload all these scripts to the S3 Bucket

![](https://cdn-images-1.medium.com/max/1600/1*aOSoz0FUY5qc74NYlAqTog.png)

#### 6. Orchestrating the Pipeline with AWS Step Functions

To automate the pipeline, I created an AWS Step Functions state machine. It orchestrates the following tasks:

1. Start the EMR Serverless application.
2. Run the PySpark jobs.
3. Stop the EMR Serverless application.

![](https://cdn-images-1.medium.com/max/1600/1*rLJwYI065nHp-8WuiCNA1Q.png)

**API parameters for Start and Stop application :**

```json
{  
  "ApplicationId": "XXXXXXX" ## update the application ID based on ur app  
}
```

**API Parameters for Start Job Run :**

```json
{  
  "Name": "Demo-bronze",  
  "ApplicationId": "XXXXX", // change this  
  "ExecutionRoleArn": "arn:aws:iam::XXXXXX:role/demo-emr-serverless-role", // change this  
  "JobDriver": {  
    "SparkSubmit": {  
      "EntryPoint": "s3://XXXXXX/rides_data_emr_bronze.py" // change this  
    }  
  }  
}
```
Here’s the JSON definition for the state machine:

```json
{  
  "Comment": "A description of my state machine",  
  "StartAt": "EMR Serverless StartApplication",  
  "States": {  
    "EMR Serverless StartApplication": {  
      "Type": "Task",  
      "Resource": "arn:aws:states:::emr-serverless:startApplication.sync",  
      "Parameters": {  
        "ApplicationId": "XXXXXXX"
      },  
      "Next": "EMR Serverless bronze zone"  
    },  
    "EMR Serverless bronze zone": {  
      "Type": "Task",  
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",  
      "Parameters": {  
        "Name": "Demo-bronze",  
        "ApplicationId": "XXXXXXXXX",  
        "ExecutionRoleArn": "arn:aws:iam::XXXXXX:role/demo-emr-serverless-role",  
        "JobDriver": {  
          "SparkSubmit": {  
            "EntryPoint": "s3://XXXXXXXX/code/rides_data_emr_bronze.py"  
          }  
        }  
      },  
      "Next": "EMR Serverless silver zone"  
    },  
    "EMR Serverless silver zone": {  
      "Type": "Task",  
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",  
      "Parameters": {  
        "Name": "demo-silver",  
        "ApplicationId": "XXXXXXXX",  
        "ExecutionRoleArn": "arn:aws:iam::XXXXXXXX:role/demo-emr-serverless-role",  
        "JobDriver": {  
          "SparkSubmit": {  
            "EntryPoint": "s3://XXXXXX/code/rides_data_silver_emr.py"  
          }  
        }  
      },  
      "Next": "EMR Serverless gold zone"  
    },  
    "EMR Serverless gold zone": {  
      "Type": "Task",  
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",  
      "Parameters": {  
        "Name": "demo-gold",  
        "ApplicationId": "XXXXXXXXX",  
        "ExecutionRoleArn": "arn:aws:iam::XXXXXX:role/demo-emr-serverless-role",  
        "JobDriver": {  
          "SparkSubmit": {  
            "EntryPoint": "s3://XXXXXXX/code/rides_data_gold_emr.py"  
          }  
        }  
      },  
      "Next": "EMR Serverless StopApplication"  
    },  
    "EMR Serverless StopApplication": {  
      "Type": "Task",  
      "Resource": "arn:aws:states:::emr-serverless:stopApplication.sync",  
      "Parameters": {  
        "ApplicationId": "XXXXXXX"  
      },  
      "End": true  
    }  
  }  
}
```

#### 7. Automating with EventBridge

The pipeline is triggered automatically whenever a new CSV file is uploaded to the raw data S3 bucket. I set up an EventBridge rule that monitors the bucket and invokes the Step Function whenever a new file is uploaded.

#### Enable event notifications in S3 :

Go to S3 bucket → properties

Turn ON the Amazon Event Bridge Notifications from the UI.

![](https://cdn-images-1.medium.com/max/1600/1*aSH1Dw3eJMOcgBu8d2rivg.png)

#### **Creating EventBridge rule**

**Navigate to EventBridge → Rules:**

- Go to **Amazon EventBridge**, and select **Rules** from the left-hand menu.
- Click **Create rule** and give it a name and description
- Choose **Event Pattern** as the event source type.
- Specify the event pattern to capture S3 bucket events such as object creation or deletion

![](https://cdn-images-1.medium.com/max/1600/1*OCo9U_tos8Wdoc1_68TbDw.png)

![](https://cdn-images-1.medium.com/max/1600/1*z6u83zbSEp6fv1uibfWogA.png)

- In the **Target** section, choose **Step Functions state machine** as the target.
- Select the desired **state machine** from the dropdown.
- Review the settings, then click **Create rule** to enable the event rule that triggers the Step Functions state machine based on S3 events.

![](https://cdn-images-1.medium.com/max/1600/1*EofvoeSBMy0wtc_iZckqGw.png)

#### Lets see all the things in action :

**Upload the CSV File:**

- Upload the CSV file to the designated S3 bucket.

![](https://cdn-images-1.medium.com/max/1600/1*_1PzRtNC5aMDU8B1z7eRUw.png)

**Event Creation and State Machine Trigger:**

- The S3 event is captured, and the EventBridge rule triggers the Step Functions state machine as configured.

![](https://cdn-images-1.medium.com/max/1600/1*MkM-JTG36mjuj1Lvyes_YA.png)

**Wait for State Machine Completion:**

- Monitor the state machine’s progress until it completes.

**Check EMR Serverless Console for Job Status:**

- While waiting, check the EMR Serverless console to track the status of the running job.

![](https://cdn-images-1.medium.com/max/1600/1*dVXINc6Wg_8GdNTsyUL6fQ.png)

![](https://cdn-images-1.medium.com/max/1600/1*gOQCRbuPOCyl9_XK21CEsQ.png)

**Verify Tables in Glue Catalog and S3:**

- Once the state machine and EMR job complete, verify that the expected tables are created in the AWS Glue Catalog and check the S3 bucket for the processed data.

![](https://cdn-images-1.medium.com/max/1600/1*YDgI5JTFRFuMnoQN8JmErA.png)

**Query the Data in Athena:**

- Finally, use **Athena** to query the data in the tables and review the results.

![](https://cdn-images-1.medium.com/max/1600/1*a-WG_UeiaMmf3Tpsrku7wQ.png)

#### Conclusion :

This project showcases how to build a fully automated and serverless data pipeline using AWS services like **EMR Serverless**, **AWS Glue**, Step Functions, and **EventBridge**. The architecture allows for seamless scaling, cost savings, and real-time event-driven data processing. By integrating **PySpark** with **Apache Iceberg**, the pipeline supports efficient data management and querying in a **lakehouse architecture**.
