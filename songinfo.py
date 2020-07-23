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
    final_df = pd.DataFrame()
    for i in range(1,2):
        id_df = spark.read.parquet("{}/{}".format(filepath, "id_data_{}.parquet".format(i)))
        id_df.createOrReplaceTempView("id_df")
        cid ="36b35ee75fec40c399220f9371d2e3b0" 
        secret = "c0ce447c51394e1198dc56fb787ee326"
        client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        names = id_df.select("SongID").toPandas()
        i=0
        # m=len(names)
        m=50
        while i<m:
            if (i+50)<m:
                song_ids = names.iloc[i:i+50]["SongID"].tolist()
            else:
                song_ids = names.iloc[i:m]["SongID"].tolist()
            i+=50
            info = sp.audio_features(song_ids)
            print(info["audio_features"])
            # for val in enumerate(info["audio_features"]):
            #     info_line = [val["danceability"],val["energy"],val["key"],val["loudness"],val["mode"],
            #                 val["speechiness"],val["acousticness"],val["instrumentalness"],val["liveness"],val["valence"],val["tempo"]]
            #     temp_info = pd.DataFrame([info_line],columns=["SongID","danceability","energy","key","loudness","mode","speechiness","acousticness",
            #                                                     "instrumentalness","liveness","valence","tempo"])
            #     final_df.append(temp_info,ignore_index = True)
        # print(final_df)
        
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
