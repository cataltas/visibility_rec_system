import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession

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

def song_info(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    id_df = spark.read.parquet("{}/{}".format(filepath, "id_data.parquet"))
    id_df.createOrReplaceTempView("id_df")
    cid ="36b35ee75fec40c399220f9371d2e3b0" 
    secret = "c0ce447c51394e1198dc56fb787ee326"
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    id_df = id_df.select("*").toPandas()
    id_df=pd.DataFrame(id_df)
    # final_df = pd.DataFrame()
    for i,val in enumerate(id_df["SongId"].iloc[0:10]):
        print(val)
        
    # df_final=spark.createDataFrame(final_df)  
    # df_final.write.parquet("{}/{}".format(filepath, "final_music.parquet"))


def main():
    song_info(filepath)    

if __name__ == "__main__":
    main()
