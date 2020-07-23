import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession

filepath = "hdfs:/user/ct2522"

def newSparkSession():
    mem = "5GB"
    spark = (SparkSession.builder.appName("Music_Project")
             .master("yarn")
             .config("sparn.executor.memory", mem)
             .config("sparn.driver.memory", mem)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def song_info(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    for i in range(1,7):
        id_df = spark.read.parquet("{}/{}".format(filepath, "id_data_{}.parquet".format(i)))
        id_df.createOrReplaceTempView("id_df")
        cid ="36b35ee75fec40c399220f9371d2e3b0" 
        secret = "c0ce447c51394e1198dc56fb787ee326"
        client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        names = id_df.select("SongID").toPandas()
        for i in range(0,len(names)):
            song_ids = names.iloc[i:i+50].tolist()
            i+=50
            print(song_ids)
        
        # names = names.select("*").toPandas()
        # final_df = pd.DataFrame()
        # print(n.iloc[0:10])
        # for i,val in enumerate(names.iloc[0:50]):
        #     print(val)
            
        # df_final=spark.createDataFrame(final_df)  
        # df_final.write.parquet("{}/{}".format(filepath, "final_music.parquet"))

def main():
    song_info(filepath)    

if __name__ == "__main__":
    main()

# sql select names then to pandas then get all the new shit then merge with sql
