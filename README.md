# Project work on Apache Spark using Python

The aim of this project is to better understand how Spark works under the hood, additionally it provides an example of how to use spark for data preprocessing and data clustering.

## Recommendation

I strongly suggest to use Jupyter Notebook, it's easy to install (especially if you install it through Anaconda) and it's definitely more interactively than launching an application with spark-submit. Furthermore, you can install some usefull features (magic commands) which also provides you some interesting information about the performance.

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

![](/spark_app.html)

 

## References 
* [Spark](https://spark.apache.org/)
* [Machine Learning Library (MLlib)](https://spark.apache.org/docs/1.1.0/mllib-guide.html)
* [Jupyter Notebook](http://jupyter.org/)

