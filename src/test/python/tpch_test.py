import pyverdict
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join, abspath

def load_table(data_path, spark):
    data_path = "'" + data_path + "'"

    #load data
    '''
        [L_OrderKey] [int] NULL,
        [L_PartKey] [int] NULL,
        [L_SuppKey] [int] NULL,
        [L_LineNumber] [int] NULL,
        [L_Quantity] [int] NULL,
        [L_ExtendedPrice] [decimal](13, 2) NULL,
        [L_Discount] [decimal](13, 2) NULL,
        [L_Tax] [decimal](13, 2) NULL,
        [L_ReturnFlag] [varchar](64) NULL,
        [L_LineStatus] [varchar](64) NULL,
        [L_ShipDate] [datetime] NULL,
        [L_CommitDate] [datetime] NULL,
        [L_ReceiptDate] [datetime] NULL,
        [L_ShipInstruct] [varchar](64) NULL,
        [L_ShipMode] [varchar](64) NULL,
        [L_Comment] [varchar](64) NULL,
        [skip] [varchar](64) NULL
    '''
    spark.sql("DROP TABLE IF EXISTS tpchTest")
    query = "CREATE TABLE IF NOT EXISTS tpchTest "
    query += ("(L_OrderKey INT, L_PartKey INT, L_SuppKey INT, L_LineNumber INT, L_Quantity INT, "
              "L_ExtendedPrice decimal(13,2), L_Discount decimal(13,2), L_Tax decimal(13,2), "
              "L_ReturnFlag varchar(64), L_LineStatus varchar(64), "
              "L_ShipDate timestamp, L_CommitDate timestamp, L_ReceiptDate timestamp,"
              "L_ShipInstruct varchar(64),L_ShipMode varchar(64),L_Comment varchar(64),"
              "skip varchar(64)) ")
    query += "USING hive"
    spark.sql(query)
    spark.sql("LOAD DATA LOCAL INPATH " + data_path + " INTO TABLE tpchTest")
    spark.sql("show tables").show()
    spark.sql("SELECT COUNT(*) FROM tpchTest").show()
    #spark.sql("DROP TABLE IF EXISTS lineitem")
    print("data table loaded")

def load_queries(query_path):
    return [f for f in listdir(query_path) if isfile(join(query_path, f))]

def __main__():
    # disable logs
    sc = SparkContext.getOrCreate()
    # sc.setLogLevel("off")

    # prepare spark session
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("show tables").show()

    #spark.sql("CREATE DATABASE IF NOT EXISTS test")
    #spark.sql("USE test")

    #set the path
    data_path = "/Users/jianxinzhang/Documents/Research/VerdictDB/verdictdb/src/test/resources/tpch_test_data/lineitem/lineitem.tbl"
    query_path = "/Users/jianxinzhang/Documents/Research/VerdictDB/verdictdb/src/test/resources/tpch_test_query"
    load_table(data_path, spark)
    spark.sql("create schema if not exists verdictdbtemp")
    verdict = pyverdict.spark(spark)
    queries = load_queries(query_path)
    for file in queries:
        with open(file, 'r') as f:
            query = f.read().replace('\n', ' ')
            verdict.sql_raw_result(query)
    spark.sql("DROP TABLE IF EXISTS lineitem")
    #spark.sql("DROP TABLE IF EXISTS verdictdbtemp")
    #spark.sql("DROP DATABASE IF EXISTS test")

__main__()