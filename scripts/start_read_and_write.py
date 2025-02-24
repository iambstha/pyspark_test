import multiprocessing
from pyspark.sql import SparkSession # type: ignore
from helper.schema_def import schema
from helper.parser import parse_fixed_width
from util.benchmark import Benchmark
from config.db_config import DB_URL, DB_PROPERTIES

benchmark = Benchmark()

def read_file(sc, spark, file_path):
    num_partitions = max(16, multiprocessing.cpu_count() * 2)
    lines = sc.textFile(file_path, minPartitions=num_partitions)
    mapped_lines = lines.map(parse_fixed_width)
    return spark.createDataFrame(mapped_lines, schema).repartition(num_partitions,"city")

def count(params, df):
    for param in params:
        print(f"{df.filter(df.state == param).count()} is the count of people from {param}")
    return df

def group_count(params, df):
    df.groupBy(params).count().show()
    return df

def run_query(query, spark):
    sqldf = spark.sql(query)
    sqldf.show()
    

def start_read_and_write(spark: SparkSession, file_path) -> None:
    print("Spark read and write started")
    sc = spark.sparkContext

    with benchmark.measure("Read File and Create DataFrame"):
        df = read_file(sc, spark, file_path)

    # with benchmark.measure("Count NY and CA"):
    #     df = count(["NY","CA"], df)

    # with benchmark.measure("Count OH"):
    #     df = count(["OH"], df)

    # with benchmark.measure("Group Count by State"):
    #     df = group_count("state", df)

    df.createOrReplaceTempView("customer_data")
    
    with benchmark.measure("Run Queries"):
        run_query("SELECT count(*) FROM customer_data", spark)

    # with benchmark.measure("Count query for NY"):
    #     run_query("SELECT count(*) FROM customer_data where state = 'NY'", spark)

    # with benchmark.measure("Count query for Otherville city and NY state"):
    #     run_query("SELECT count(*) FROM customer_data where state = 'NY' and city = 'Otherville'", spark)

    # with benchmark.measure("Limit 10 query for NY state"):
    #     run_query("SELECT * FROM customer_data where state = 'NY' limit 10", spark)

    with benchmark.measure("Add to DB"):

        df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "customer_data") \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .option("batchsize", 100000) \
            .option("numPartitions", str(max(16, multiprocessing.cpu_count() * 2))) \
            .mode("append") \
            .save()
