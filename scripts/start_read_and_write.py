
import os
import sys
import multiprocessing
from pyspark.sql import SparkSession # type: ignore
import time
import functools
from helper.schema_def import schema
from helper.parser import parse_fixed_width

def read_file(sc, spark):
    file_path = "data/large_fixed_width_data_1gb.txt"
    num_partitions = max(16, multiprocessing.cpu_count() * 2)
    read_start_time  =time.time()
    lines = sc.textFile(file_path, minPartitions=num_partitions)
    mapped_lines = lines.map(parse_fixed_width)
    df = spark.createDataFrame(mapped_lines, schema).repartition(num_partitions,"city")
    elapsed_time = time.time() - read_start_time
    print(f"📊 File read completed in {elapsed_time:.2f} seconds!")
    return df

def count(params, df):
    read_start_time = time.time()
    for param in params:
        print(f"{df.filter(df.state == param).count()} is the count of people from {param}")
        elapsed_time = time.time() - read_start_time
        print(f"📊 Count query by filter completed in {elapsed_time:.2f} seconds! for param {param}")
    return df

def group_count(params, df):
    read_start_time = time.time()
    df.groupBy(params).count().show()
    elapsed_time = time.time() - read_start_time
    print(f"📊 Group by query completed in {elapsed_time:.2f} seconds!")
    return df

def start_read_and_write(spark: SparkSession) -> None:
    print("Spark read and write started")
    sc = spark.sparkContext
    df = read_file(sc, spark)
    df = count(["NY","CA"], df)
    df = group_count("state", df)

    # read_start_time = time.time()
    # df.createOrReplaceTempView("customer_data")
    # elapsed_time = time.time() - read_start_time
    # print(f"📊 Temp view created in {elapsed_time:.2f} seconds!")

    # read_start_time = time.time()
    # sqldf = spark.sql("SELECT count(*) FROM customer_data where state = 'NY'")
    # # sqldf.filter(sqldf.state == "NY").show()
    # sqldf.show()
    # elapsed_time = time.time() - read_start_time
    # print(f"📊 Count query for one filter completed in {elapsed_time:.2f} seconds!")

    # read_start_time = time.time()
    # sqldf = spark.sql("SELECT count(*) FROM customer_data where state = 'NY' and city = 'Otherville'")
    # sqldf.show()
    # elapsed_time = time.time() - read_start_time
    # print(f"📊 Count query for multiple filter completed in {elapsed_time:.2f} seconds!")

    # read_start_time = time.time()
    # sqldf = spark.sql("SELECT * FROM customer_data where state = 'NY' limit 10")
    # sqldf.show()
    # elapsed_time = time.time() - read_start_time
    # print(f"📊 Show query for multiple filter completed in {elapsed_time:.2f} seconds!")

    # names = spark.sql("SELECT first_name FROM customer_data")
    # each_name = names.rdd.map(lambda x: "Name: " + x.first_name).collect()
    # for name in each_name:
    #     print(name)
    # df.persist(StorageLevel.MEMORY_AND_DISK)
    # df.printSchema()
    # df.select("first_name").show()
    # df.createOrReplaceTempView("customer_data")
    # df.show(5)
    # spark.sql("SELECT * FROM customer_data LIMIT 5").show()
