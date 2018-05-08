
## Example
How to transform nominal values to numerical values. 


```python
import findspark
from pyspark.sql import SparkSession

findspark.init("/usr/local/spark")
spark = SparkSession.builder \
   .master("local[*]") \
   .appName("Test") \
   .getOrCreate()
sc = spark.sparkContext
```


```python
import os

file_path = "/Users/enricodamini/Desktop/data"
files = os.listdir(file_path)[1:]
```


```python
def apply_preprocessing(rdd) :
    '''
    This function applies some transformations in order to have a dataset with this shape: ((plate,path),times)
     - **parameters**, **types**, **return** and **return types**::
          :param rdd: RDD to transform
          :type rdd: pyspark.rdd.RDD
          :return: return the transformed RDD 
          :rtype: pyspark.rdd.RDD
    '''
    header = rdd.first()
    rdd = rdd.filter(lambda lines : lines!=header)
    rdd = rdd.map(lambda lines : lines.split(";")).map(lambda (l1,l2,l3,l4,l5) : (l1,str(l2))).reduceByKey(lambda g1,g2: str(g1)+"-"+str(g2))
    rdd = rdd.map(lambda (plate,path) : ((plate,path),1))
    return rdd 
```


```python
def update(rdd,rdd_new) :
    '''
    Given two RDDs, this function provides a new RDD, which is the union of the RDDs.
    Additionally, the field "times" of the new RDD has been updated. 
     - **parameters**, **types**, **return** and **return types**::
          :param rdd,rdd_new: one of the RDDs to compare
          :type rdd,rdd_new: pyspark.rdd.RDD
          :return: return the new RDD 
          :rtype: pyspark.rdd.RDD
    '''
    result = rdd.union(rdd_new)
    result = result.reduceByKey(lambda a,b : a+b)
    return result
```


```python
def main() :
    rdd_info = sc.textFile(files[0])
    rdd_info = apply_preprocessing(rdd_info)
    
    for file_path in files[1:] : 
        rdd_new = sc.textFile(file_path)
        rdd_new = apply_preprocessing(rdd_new)
        rdd_info = update(rdd_info,rdd_new)
    return rdd_info    
```


```python
rdd = main()
```


```python
rdd = rdd.sortByKey() 
rdd.take(5)
```




    [((u'10', '20-2-2-13-8'), 1),
     ((u'100000', '8-5-25'), 1),
     ((u'100001', '8-5-25'), 2),
     ((u'1000012', '3-13'), 1),
     ((u'1000014', '5-23-10'), 1)]



I need to know the total number of paths for the generate an unique ID (which must be a number):
```python 
num_path = rdd.map(lambda ((plate,path),times) : (path,1)).keys().distinct().count()
print num
```




    17404




```python
def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
```


```python
import sys 

def get_hash(path) :
    return (java_string_hashcode(path) & sys.maxint) % num_path 
    
```


```python
rdd_final  = rdd.map(lambda ((plate,path),times) : (plate,get_hash(path),times ))
```


```python
rdd_final.take(5)
```




    [(u'10', 12630, 1),
     (u'100000', 13676, 1),
     (u'100001', 13676, 2),
     (u'1000012', 15200, 1),
     (u'1000014', 4931, 1)]




```python
rdd.take(5)
```




    [((u'10', '20-2-2-13-8'), 1),
     ((u'100000', '8-5-25'), 1),
     ((u'100001', '8-5-25'), 2),
     ((u'1000012', '3-13'), 1),
     ((u'1000014', '5-23-10'), 1)]

The two identical paths (8-5-25) are assigned at the same ID, which is  13676


