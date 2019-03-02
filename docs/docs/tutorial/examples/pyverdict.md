# Pyverdict with Pyspark


This is a simple example program of Pyverdict with Pyspark. To run pyspark, hadoop and hive must be installed and properly configured.

## Install pyverdict
```bash
pip install pyverdict
```
## Start Hadoop server in terminal
```bash
$HADOOP_HOME/sbin/start-all.sh
```
``` $HAOOP_HOME ```  is the directory where your Hadoop is installed.
To check if the hadoop server have already started, type ```jps```. If you can see Namenode and Datanode in the list, then the server is properly launched. For example,
```bash
483 UserClient
2548 
3398 SecondaryNameNode
3592 ResourceManager
3161 NameNode
3689 NodeManager
3804 Jps
3262 DataNode
```
Sometimes you may want to restart you hadoop server. You can use:
```bash
$HADOOP_HOME/sbin/stop-all.sh
$HADOOP_HOME/sbin/start-all.sh
```
To format your namenode, use ```hdfs namenode -format```

## Import packages
```python
import pyverdict
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os.path import abspath
```
## disable logs
```python
sc = SparkContext.getOrCreate()
sc.setLogLevel("off")
```

## Start a Spark session
```python
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
.builder \
.appName("Pyverdict example") \
.config("spark.sql.warehouse.dir", warehouse_location) \
.enableHiveSupport() \
.getOrCreate()
```
## Create Hive Table
```python
spark.sql("DROP TABLE IF EXISTS example")
spark.sql("CREATE TABLE IF NOT EXISTS example (key INT, value STRING) USING hive")
```
## Load data
```python
spark.sql("LOAD DATA LOCAL INPATH 'PATH/kv1.txt' INTO TABLE example")
```

## Copy spark into verdict
```python
verdict = pyverdict.spark(spark)
```
## Query through pyverdict
Now we can query by pyverdict, the syntax is the same as SQL. Here are some examples:  
#### 1. count the number of rows.
```python
query = "SELECT count(*) FROM default.example"
res = verdict.sql_raw_result(query)
print(res.to_df())
```
Output:
```
1 row(s) in the result (2.084 seconds)
c2
0  500
```
#### 2. select rows ending with 00.
```python
query = "SELECT * FROM default.example WHERE value LIKE '%00' "
res = verdict.sql_raw_result(query)
print(res.to_df())
```
Output:
```
5 row(s) in the result (0.259 seconds)
key    value
0  100  val_100
1  200  val_200
2  100  val_100
3  400  val_400
4  200  val_200
```
#### 3. insert a row by pyspark and select it.
```python
query = "INSERT INTO default.example VALUES (999, 'val_999') "
spark.sql(query)
query = "SELECT * FROM default.example WHERE value LIKE '%999' "
res = verdict.sql_raw_result(query)
print(res.to_df())
```
Output:
```
1 row(s) in the result (0.142 seconds)
key    value
0  999  val_999
```
## Drop the table
```python
spark.sql("DROP TABLE IF EXISTS default.example")
```

## Stop hadoop server in terminal
```bash
$HADOOP_HOME/sbin/stop-all.sh
```
## Complete python code
```python
import pyverdict
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os.path import abspath

#disable logs
sc = SparkContext.getOrCreate()
sc.setLogLevel("off")

#prepare spark session
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
.builder \
.appName("Pyverdict example") \
.config("spark.sql.warehouse.dir", warehouse_location) \
.enableHiveSupport() \
.getOrCreate()

#load data
spark.sql("DROP TABLE IF EXISTS example")
spark.sql("CREATE TABLE IF NOT EXISTS example (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH '/Users/jianxinzhang/Documents/Research/VerdictDB/verdictdb-tutorial/pyverdict_on_spark/kv1.txt' INTO TABLE example")

#exectue queries
verdict = pyverdict.spark(spark)

query = "SELECT count(*) FROM default.example"
res = verdict.sql_raw_result(query)
print(res.to_df())

query = "SELECT * FROM default.example WHERE value LIKE '%00' "
res = verdict.sql_raw_result(query)
print(res.to_df())

query = "INSERT INTO default.example VALUES (999, 'val_999') "
spark.sql(query)
query = "SELECT * FROM default.example WHERE value LIKE '%999' "
res = verdict.sql_raw_result(query)
print(res.to_df())

spark.sql("DROP TABLE IF EXISTS default.example")
```
