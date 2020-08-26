import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import seaborn as sns
import matplotlib.pyplot as plt


filepath = "hdfs:/user/ct2522"

def newSparkSession():
    # return SparkSession.builder.getOrCreate()
    mem = "15GB"
    spark = (SparkSession.builder.appName("Music_Project")
             .master("yarn")
             .config("sparn.executor.memory", mem)
             .config("sparn.driver.memory", mem)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def prepare(filepath):
    df = spark.read.parquet("{}/{}".format(filepath, "final.parquet"))
    df = df.dropDuplicates()
    df= df.dropna()
    df= df.dropDuplicates(["SongID"])
    df = df.withColumn("Year", df["Year"].cast(IntegerType()))
    col ="Gender"
    df = df.withColumn(col, f.when(f.col(col)=="Female",1).when(f.col(col)=="Male",0))

def correlation(data):
    dfe = data.drop(*["Artist","SongID"])
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=df2.columns, outputCol=vector_col)
    df_vector = assembler.transform(df2).select(vector_col)
    matrix = Correlation.corr(df_vector, vector_col)
    mat_array = np.reshape(matrix.collect()[0]["pearson({})".format(vector_col)].values,[14,14])
    m=pd.DataFrame(mat_array,columns=dfe.columns,index=dfe.columns)
    ax = sns.heatmap(m)
    plt.show()



def main():
    data =prepare(filepath)  
    correlation(data)

if __name__ == "__main__":
    main()


# 5992041 songs,  5207723 after drops, 4042270 >10 popularity (67%, 902552 fem aka 22%, 3139694 male), 480 >80 popularity(87 female (18%), 394 male,  most data since 2006, 1160218 feml(22%), 4047503 males
# more artists male than female altogether and within that the stats are relatively close to proportions 
# correlation matrix, train test split both in a new funciton