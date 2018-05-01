import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark import sql
import sh
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

#Note: you  need to change the path to the spark repository, to HDFS and also the master's URL 

findspark.init("/ <path_to_spark> /spark") 
conf = SparkConf().setAppName("SO_project").setMaster("spark:// <master_name> :7077")
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)
hdfsdir = '/ <path_to_hdfs> /hdfs/dataset'
files = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][2:]   

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

def main() :
    '''
     basically, it reads the first file (which means the first day) of the dataset and 
     it adds the information of the other files (the other days) by using .union 
     - **parameters**, **types**, **return** and **return types**::
        :return: return the transformed RDD 
        :rtype: pyspark.rdd.RDD
    '''
    rdd_tot = sc.textFile(files[0])
    rdd_tot = apply_preprocessing(rdd_tot)
    
    for file_path in files[1:] : 
        rdd_new = sc.textFile(file_path)
        rdd_new = apply_preprocessing(rdd_new)
        rdd_tot = rdd_tot.union(rdd_new)
    return rdd_tot 

rdd = main()

#what I'm doing here is to change the shape of the rdd (which is the summary of the all dataset): 
#1) Firstly I need to convert the categorical nominal values (the gates) to numerical values and then
#I need to count how many times a gate compares with a plate.  
#2)I have to work with spark.DataFrame (which are optimazed)  
df = (rdd.map(lambda (plate,gate) : Row(plate=plate,gate=gate))).toDF()
types = df.select("gate").distinct().rdd.flatMap(lambda x: x).collect() #action
types_expr = [F.when(F.col("gate") == ty, int(1) ).otherwise(int(0)).alias("gate_" + str(ty)) for ty in types]
df = df.select("plate", *types_expr)
df= df.groupBy("plate").sum() #action

#CLUSTERING
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

