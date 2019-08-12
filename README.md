# PySparkML
PySparkML
PySpark in Google Colab
Apache Spark was build to analyze Big Data with faster speed. One of the important features that Apache Spark offers is the ability to run the computations in memory. It is also considered to be more efficient than MapReduce for the complex application running on Disk.
Spark is designed to be highly accessible, offering simple APIs in Python, Java, Scala, and SQL, and rich built-in libraries. It also integrates closely with other Big Data tools. In particular, Spark can run in Hadoop clusters and access any Hadoop data source, including Cassandra.
PySpark is the interface that gives access to Spark using the Python programming language. PySpark is an API developed in python for spark programming and writing spark applications in Python style, although the underlying execution model is the same for all the API languages.

Colab by Google is based on Jupyter Notebook which is an incredibly powerful tool that leverages google docs features. Since it runs on google server, we don't need to install anything in our system locally, be it Spark or deep learning model. The most attractive features of Colab are the free GPU and TPU support! Since the GPU support runs on Google's own server, it is, in fact, faster than some commercially available GPUs like the Nvidia 1050Ti.

Running Pyspark in Colab
To run spark in Colab, first we need to install all the dependencies in Colab environment such as Apache Spark 2.3.2 with hadoop 2.7, Java 8 and Findspark in order to locate the spark in the system. The tools installation can be carried out inside the Jupyter Notebook of the Colab.
Follow the steps to install the dependencies:

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://apache.osuosl.org/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
!tar xf spark-2.3.2-bin-hadoop2.7.tgz
!pip install -q findspark

Now that we have installed Spark and Java in Colab, it is time to set the environment path that enables us to run PySpark in our Colab environment. Set the location of Java and Spark by running the following code:

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.2-bin-hadoop2.7"

We can run a local spark session to test our installation:

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
