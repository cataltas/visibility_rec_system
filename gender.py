#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession

filepath = "hdfs:/user/ct2522"

def newSparkSession():
    # return SparkSession.builder.getOrCreate()
    mem = "7GB"
    spark = (SparkSession.builder.appName("Music_Project")
             .master("yarn")
             .config("sparn.executor.memory", mem)
             .config("sparn.driver.memory", mem)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def prepare_gender(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()

    # This loads the JSON file and returns the gender dataframe
    artists = spark.read.json("{}/{}".format(filepath, "artist"))
    artists.createOrReplaceTempView("artists")
    results=spark.sql("SELECT name,gender FROM artists")
    female=results.filter(results.gender=="Female").select(results.name,results.gender)
    male = results.filter(results.gender=="Male").select(results.name,results.gender)
    gender = female.union(male) 
    # Convert to Pandas for cleaning
    df=gender.select("*").toPandas()
    df=pd.DataFrame(df)
    # Remove duplicated data
    df=df.drop_duplicates()
    # Remove names with both genders because unlabelled 
    df=df.drop_duplicates(subset="name",keep=False)
    df_s=spark.createDataFrame(df)  
    df_s.write.parquet("{}/{}".format(filepath, "gender_df.parquet"))
# male count = 531 089, female count = 148 097, lost instances = 984 124 
# male count = 498 107, female count = 142 557, total = 640 664




def main():
    prepare_gender(filepath)    

if __name__ == "__main__":
    main()
