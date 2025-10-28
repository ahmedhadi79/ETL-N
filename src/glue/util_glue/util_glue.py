import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# SQL Query for Athena
athena_query = """
SELECT
    customernumber,
    country,
    glcode,
    description,
    balancedate,
    currentvalueamountbalance,
    timestamp_extracted,
    isocurrencycode,
    foreigncurrencyamountbalance,
    date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customernumber,
                            country,
                            glcode,
                            description,
                            balancedate,
                            currentvalueamountbalance,
                            isocurrencycode,
                            foreigncurrencyamountbalance,
                            date
               ORDER BY timestamp_extracted DESC
           ) AS row_num
    FROM datalake_raw.imal_reporting_currentaccountbalancebyday
)
WHERE row_num = 1
"""

# Function to Read Data from Athena
def read_athena(sql: str) -> DataFrame:
    """
    Reads data from Athena via Spark SQL.
    """
    logger.info(f"Reading from Athena with query: {sql}")
    # Execute query and load into a DataFrame
    df = spark.sql(sql)
    logger.info("Successfully read data from Athena.")
    return df

# Function to Write Data as Parquet to S3 with Partitioning
def write_as_parquet_s3(df: DataFrame, s3_bucket: str, target_file_size_mb: int = 127):
    """
    Writes the DataFrame to Parquet files in the specified S3 bucket, partitioned by 'date'.
    """
    table_name = "imal_reporting_currentaccountbalancebyday"
    output_path = f"s3://{s3_bucket}/{table_name}"
    try:
        logger.info(f"Writing data to S3 bucket at: {output_path}")

        # Estimate total data size in bytes
        total_data_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
        total_data_size_mb = total_data_size_bytes / (1024 * 1024)  # Convert to MB
        logger.info(f"Estimated total data size: {total_data_size_mb:.2f} MB.")

        # Calculate the optimal number of partitions
        num_partitions = max(1, int(total_data_size_bytes / (target_file_size_mb * 1024 * 1024)))
        logger.info(f"Calculated partitions: {num_partitions}, target file size: {target_file_size_mb} MB.")

        # Repartition the DataFrame
        if num_partitions < df.rdd.getNumPartitions():
            logger.info("Reducing partitions using coalesce to achieve larger file sizes.")
            df = df.coalesce(num_partitions)
        else:
            logger.info("Increasing partitions using repartition for better data distribution.")
            df = df.repartition(num_partitions)

        # Write the DataFrame to S3 in Parquet format, partitioned by 'date'
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("date") \
            .parquet(output_path)

        logger.info("Data successfully written to S3.")
    except Exception as e:
        logger.error(f"Failed to write data to S3: {e}")
        raise


# Main Job Execution
if __name__ == "__main__":
    # @params: [JOB_NAME, S3_RAW]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_RAW"])
    s3_bucket = args["S3_RAW"]

    # Initialize Glue Job
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Set Spark Configuration
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Process Data
    try:
        logger.info("Starting data processing pipeline.")
        athena_df = read_athena(athena_query)
        # Ensure 'date' column is available and used for partitioning
        if "date" in athena_df.columns:
            write_as_parquet_s3(athena_df, s3_bucket, target_file_size_mb=127)
        else:
            logger.error("'date' column not found in the DataFrame.")
            raise ValueError("The required column 'date' is missing.")
        logger.info("Data processing completed successfully.")
    except Exception as e:
        logger.error(f"Error during job execution: {e}")
        raise

    # Commit Glue Job
    job.commit()
