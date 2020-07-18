import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession

filepath = "hdfs:/user/ct2522"

def newSparkSession():
    # return SparkSession.builder.getOrCreate()
    mem = "30GB"
    spark = (SparkSession.builder.appName("Music_Project")
             .master("yarn")
             .config("sparn.executor.memory", mem)
             .config("sparn.driver.memory", mem)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def song_info(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    id_df = spark.read.parquet("{}/{}".format(filepath, "id_data.parquet"))
    id_df.createOrReplaceTempView("id_df")
    cid ="36b35ee75fec40c399220f9371d2e3b0" 
    secret = "c0ce447c51394e1198dc56fb787ee326"
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    final_df = pd.DataFrame()
    id_df.select(id_df["SongId"]).collect()
    # for i,val in enumerate(id_df.select(id_df["SongId"]).collect()):
    #     print(val)
        
    # df_final=spark.createDataFrame(final_df)  
    # df_final.write.parquet("{}/{}".format(filepath, "final_music.parquet"))

def main():
    song_info(filepath)    

if __name__ == "__main__":
    main()
