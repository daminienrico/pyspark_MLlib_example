# Project work on Apache Spark (Python)

## About

The aim of this project is to better understand how Spark works under the hood. With this purpuse, it has been provided an example of how to use spark for data preprocessing and data clustering. 
It has been used Spark StandAlone mode, see [here](https://spark.apache.org/docs/latest/spark-standalone.html) how to set it up.  

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

Getting the file names in the hdfs directory: 

```python
import sh
hdfsdir = '/<path_to>/hdfs/dataset'
files = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][2:]   
```

When you work with unstructered data and you want to perform low-level transformations, you can use RDD instead of spark.Dataframe. As a rule of thumbs, it is better to use spark.Dataframe because they are optimized and you can achieve better performances. However, in this case we use an RDD because they are not heavy computiations. 

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
As we can see from the ouput of the magic command %time it does not take long, indeed they are not actions.
```python
%time rdd = main() 
```
```
CPU times: user 123 ms, sys: 25.4 ms, total: 148 ms
Wall time: 13.3 s
```
When we call the function count() we need to know the actual number of records, that's why takes long.
```python
%prun count= rdd.count()
print count 
``` 
``` 
7264755
``` 
What I'm doing above is to change the shape of the rdd (which is the summary of the all dataset). Firstly I need to convert the categorical nominal values (the gates) to numerical values and then I need to count how many times a gate compares with a plate. As I said before, it is better to work with spark.DataFrame because they are fully optimazed. 

```python
from pyspark.sql import Row
import pyspark.sql.functions as F 

df = (rdd.map(lambda (plate,gate) : Row(plate=plate,gate=gate))).toDF()
types = df.select("gate").distinct().rdd.flatMap(lambda x: x).collect() #action
types_expr = [F.when(F.col("gate") == ty, int(1) ).otherwise(int(0)).alias("gate_" + str(ty)) for ty in types]
df = df.select("plate", *types_expr)
df= df.groupBy("plate").sum() 
```
%prun is a Jupyter command which shows the performance

```python
%prun df.show() 
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
    
     

As we can see the number of records is definitely reduced: 

```python
df.count() #action 
```




    1594237


We are ready to cluster the data, but first we need to transform the DataFrame to an RDD. The first column is delited because the plate (which is a number) is not a meaningful information for KMeans. 
It will be necessary to tune the right number of clusters, dependig on the WSSSE.

```python
#CLUSTERING
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

rdd = df.rdd.map(tuple)
rdd = rdd.map(lambda row : row[1:])

# Build the model (cluster the data)
%prun clusters = KMeans.train(rdd,10, maxIterations=10, runs=10, initializationMode="random")
```
The centers of each cluster:

```python
for i,center in enumerate(clusters.clusterCenters) :
    print("cluster "+ str(i)+": "+str(center)+"\n")
```

```
   cluster 0: [0.04987755 0.05158012 0.05980438 0.08206529 0.07774918 0.09975163
 0.08255323 0.07082299 0.14388402 0.0008565  0.06959793 0.05557751
 0.05847849 0.00015067 0.07053904 0.08171296 0.06136208 0.06020887
 0.06298468 0.04236028 0.07803777 0.10334453 0.07373788]

cluster 1: [0.02690759 0.02342957 0.50527022 0.02666208 0.90343383 0.02802874
 0.01769289 0.63970997 0.84339095 0.9047923  0.01804478 0.03359357
 0.48867393 0.19658581 0.02233297 0.0299437  0.92038201 0.01590068
 0.03826639 0.01748011 0.92499754 0.02306131 0.03936299]

cluster 2: [0.24339769 0.57968211 0.02308649 0.5793699  0.22045058 0.04366418
 0.07452235 0.05482553 0.04184112 0.04558759 0.37397767 0.3504396
 0.02059999 0.14390447 0.61849037 0.0448238  0.05977622 0.25415763
 0.87051274 0.17705958 0.10021241 0.04004036 0.91119424]

cluster 3: [0.12795567 0.75330198 0.12628052 0.27839701 0.3320018  0.97236003
 4.03240771 0.20295084 0.18800335 0.2828426  0.02847755 0.20636557
 0.09728755 0.08923394 0.30455512 3.70452935 0.29985181 0.05122093
 0.37426712 0.08092262 0.86044714 0.96636815 0.3277495 ]

cluster 4: [1.46433783 2.59959869 1.46680044 3.28666545 3.05335644 0.28356439
 0.5902043  2.03666545 2.89830354 3.2271981  0.41335279 2.05618387
 1.21917184 1.15432324 3.0745166  0.53338198 3.11665451 0.69253922
 3.13872674 0.9014958  2.92831084 0.2824699  3.12294783]

cluster 5: [1.38922785 0.21756435 1.38254454 0.08348508 0.49255637 0.12786528
 0.21863443 1.31396549 0.06150149 0.06831622 0.29703193 1.33131207
 1.11410442 0.76154092 0.08463026 0.2146357  0.07199579 0.46475304
 0.38314529 0.95456849 0.27395949 0.13131958 0.48617343]

cluster 6: [0.61666684 0.93974425 0.61697357 1.08009773 1.18123261 0.05457603
 0.07928332 0.82730282 0.97331486 1.06923541 0.20272457 0.84317852
 0.54582377 0.38748982 1.05217511 0.07110749 1.07191132 0.24163643
 1.12567295 0.44138894 1.08970142 0.05356066 1.20335918]

cluster 7: [0.05296729 0.01699485 0.01742517 0.05599041 0.18040145 0.02146145
 0.03487758 0.03915897 0.15057875 0.48886892 0.07488629 0.07893891
 0.01647193 0.59771223 0.02118909 0.01823678 0.13426478 0.08016995
 0.02822671 0.032056   0.08570962 0.01085601 0.0385598 ]

cluster 8: [0.01545851 0.48267199 0.01275401 1.25420823 0.23178896 0.0913323
 0.25111949 0.02935048 1.33197369 1.1618562  0.10005173 0.03136038
 0.01041898 0.14698884 1.02193157 0.23685805 1.01548807 0.09959359
 0.24294687 0.00715289 0.60490652 0.08809577 0.22108919]

cluster 9: [6.48491879 1.16264501 6.3837587  0.70139211 2.30023202 0.20324826
 0.66589327 5.91113689 0.63016241 0.75336427 0.80812065 5.9350348
 4.84640371 3.0712297  0.73225058 0.63225058 0.77935035 1.74663573
 1.80904872 4.14802784 1.3962877  0.19605568 2.29025522]
```
At the end we can compute the WSSSE:

```python
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = rdd.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
```

## Comment

The results are not the best we can achieved due to the data, however if we compare this example with [this one](https://github.com/daminienrico/py_analysis_clustering_motorway), which basically does the same things (especially the pre-processing), we can see that we have better performance by using Spark and the code is also more understandable. Furthermore, the target of this project was to provide an example and to understand the way of programming in a Spark environments, which is definitely one of the most promising computing framework, especially for Data Science at scale.

 

## References 
* [Spark](https://spark.apache.org/)
* [Machine Learning Library (MLlib)](https://spark.apache.org/docs/1.1.0/mllib-guide.html)
* [Jupyter Notebook](http://jupyter.org/)


