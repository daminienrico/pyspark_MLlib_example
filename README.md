# Project work on Apache Spark using Python

The aim of this project is to better understand how Spark works under the hood. Additionally it has been provided an example of how to use spark for data preprocessing and data clustering.

## Recommendation

I strongly suggest using Jupyter Notebook, it's easy to install (especially if you install it through Anaconda) and it's definitely more interactively than launching an application with spark-submit. Furthermore, you can install some usefull features (magic commands) which also provide some interesting information about the performance.

## Dataset

The dataset is made up of several files: totally there are 7 months and there is a file (ex. 01.06.2016.csv) for each day of the month, which is about 500.000 records. 
A record is structured in this way: 

```
targa;varco;corsia;timestamp;nazione
559784;18;1.0;2016-06-07 10:22:59;IT
```

The headers of the dataset are reported in italian language. Then, the translation follows in order to make clear to any researcher the meaning of every column of the dataset.

targa -> plate
varco -> gate
corsia -> lane
timestamp -> timestamp
nazione -> nationality

Some csv files contains "parziale" in the name. It means "partial", indicating that some problems occurred in that day, causing out-of-service moments.

**N.B.** the dataset folder contains just few examples. 

## Example

Firstly, we need to process and transform the data into a useful shape, which can be done by exploiting the powerful transformation functions provided by Spark. 
Normally, the pre-processing takes really long if it is not done in a parallel way: that's why we use Spark!
We are going to cluster by taking into account the cars which have been driven through the same gates and also by taking into account the number of times they drive through each gate. 

```python
import findspark
findspark.init("/<path_to>/spark")
```

```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark import sql

conf = SparkConf().setAppName("SO_project").setMaster("spark://<master-server>-0:7077")
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)
```


```python
def apply_preprocessing(rdd) :
    '''
    This function applies some transformations in order to have a dataset with this shape: ((plate,gate)) 
     - **parameters**, **types**, **return** and **return types**::
          :param rdd: RDD to transform
          :type rdd: pyspark.rdd.RDD
          :return: return the transformed RDD 
          :rtype: pyspark.rdd.RDD
    '''
    header = rdd.first()
    rdd = rdd.filter(lambda lines : lines!=header)
    rdd = rdd.map(lambda lines : lines.split(";")).map(lambda (l1,l2,l3,l4,l5) : ((l1,int(l2))))
    return rdd
```

Getting the file names in the hdfs directory. 

```python
import sh
hdfsdir = '/<path_to>/hdfs/dataset'
files = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][2:]   
```


```python
def main() :
    rdd_tot = sc.textFile(files[0])
    rdd_tot = apply_preprocessing(rdd_tot)
    
    for file_path in files[1:] : 
        rdd_new = sc.textFile(file_path)
        rdd_new = apply_preprocessing(rdd_new)
        rdd_tot = rdd_tot.union(rdd_new)
    return rdd_tot 
```


```python
 rdd = main()
```

What I'm doing here is to change the shape of the rdd (which is the summary of the all dataset). Firstly I need to convert the categorical nominal values (the gates) to numerical values and then I need to count how many times a gate compares with a plate. It is better to work with spark.DataFrame (which are optimazed). 

```python
from pyspark.sql import Row
import pyspark.sql.functions as F 

df = (rdd.map(lambda (plate,gate) : Row(plate=plate,gate=gate))).toDF()
types = df.select("gate").distinct().rdd.flatMap(lambda x: x).collect() #action
types_expr = [F.when(F.col("gate") == ty, int(1) ).otherwise(int(0)).alias("gate_" + str(ty)) for ty in types]
df = df.select("plate", *types_expr)
%prun df= df.groupBy("plate").sum() #action 
```
%prun is a Jupyter command which shows the performance

```python
%prun df.show() #action 
```

    +-------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    |  plate|sum(gate_1)|sum(gate_2)|sum(gate_3)|sum(gate_4)|sum(gate_5)|sum(gate_6)|sum(gate_7)|sum(gate_8)|sum(gate_9)|sum(gate_10)|sum(gate_11)|sum(gate_12)|sum(gate_13)|sum(gate_14)|sum(gate_16)|sum(gate_17)|sum(gate_18)|sum(gate_19)|sum(gate_20)|sum(gate_22)|sum(gate_23)|sum(gate_24)|sum(gate_25)|
    +-------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    |1147201|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           1|           0|           0|           0|
    |8529873|          0|          0|          0|          1|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |2793645|          0|          0|          0|          0|          0|          0|          0|          0|          1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |3604472|          0|          0|          0|          0|          1|          0|          0|          0|          0|           0|           1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           1|
    |8530376|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           1|           0|           0|           0|           0|           0|
    |8530420|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           1|           1|           0|           0|           0|           0|
    |8530538|          0|          0|          0|          0|          0|          0|          0|          0|          1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8530871|          0|          0|          0|          0|          1|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |1050640|          0|          0|          1|          0|          0|          0|          0|          0|          0|           1|           0|           0|           2|           0|           0|           0|           1|           0|           0|           0|           0|           0|           0|
    |8530989|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           1|           0|           0|           0|           0|           0|           0|
    | 494038|          0|          0|          0|          0|          0|          0|          0|          1|          0|           0|           0|           0|           0|           0|           0|           0|           1|           0|           0|           0|           0|           0|           0|
    | 150002|          0|          0|          1|          1|          1|          0|          0|          2|          1|           2|           0|           0|           2|           0|           1|           0|           0|           0|           1|           0|           0|           0|           1|
    |8531267|          0|          0|          0|          0|          0|          0|          0|          1|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8531311|          0|          0|          0|          0|          1|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8531429|          0|          0|          1|          0|          0|          0|          0|          0|          0|           0|           1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |5417456|          0|          0|          0|          0|          0|          0|          0|          1|          0|           0|           1|           0|           0|           1|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8531762|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           1|
    |8531924|          0|          0|          0|          0|          0|          0|          0|          0|          0|           0|           0|           1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8532040|          0|          0|          0|          0|          0|          0|          0|          0|          0|           1|           0|           0|           1|           0|           0|           0|           0|           0|           0|           0|           0|           0|           0|
    |8532158|          0|          1|          0|          0|          0|          0|          0|          0|          0|           0|           0|           0|           0|           0|           1|           0|           0|           1|           0|           0|           0|           0|           1|
    +-------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    only showing top 20 rows
    
     


```python
df.count() #action 
```




    1594237


We are ready to cluster the data, but first we need to transform the DataFrame to an RDD. The first column is delited because the plate (which is a number) is not a meaningful information for KMeans.  

```python
#CLUSTERING
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

rdd = df.rdd.map(tuple)
rdd = rdd.map(lambda row : row[1:])

# Build the model (cluster the data)
clusters = KMeans.train(rdd,10, maxIterations=10, runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = rdd.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
```

    /opt/spark/python/pyspark/mllib/clustering.py:176: UserWarning: Support for runs is deprecated in 1.6.0. This param will have no effect in 1.7.0.
      "Support for runs is deprecated in 1.6.0. This param will have no effect in 1.7.0.")


    Within Set Sum of Squared Error = 2429782.06875


## Comment

The results are not the best we can achieved due to the data, however the target of this project was to provide an example and to understand the way of programming in a Spark environments, which is definitely one of the most promising computing framework, especially for Data Science at scale.

 

## References 
* [Spark](https://spark.apache.org/)
* [Machine Learning Library (MLlib)](https://spark.apache.org/docs/1.1.0/mllib-guide.html)
* [Jupyter Notebook](http://jupyter.org/)

