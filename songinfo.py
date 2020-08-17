import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys, getopt, random
import numpy as np
import os
import pandas as pd
from pyspark.sql import SparkSession

filepath = "hdfs:/user/ct2522"

def newSparkSession():
    mem = "10GB"
    spark = (SparkSession.builder.appName("Music_Project")
             .master("yarn")
             .config("sparn.executor.memory", mem)
             .config("sparn.driver.memory", mem)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# def song_info(file_path,sparkSession=None):
#     spark = sparkSession or newSparkSession()
#     final_df = pd.DataFrame()
#     id_df = spark.read.parquet("{}/{}".format(filepath, "id_data_2.parquet"))
#     id_df.createOrReplaceTempView("id_df")
#     cid ="36b35ee75fec40c399220f9371d2e3b0" 
#     secret = "c0ce447c51394e1198dc56fb787ee326"
#     client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
#     sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
#     names = id_df.select("SongID").toPandas()
#     # i=0
#     i= int(np.floor(len(names)/2))
#     m = len(names)
#     while i<m:
#         if (i+50)<m:
#             song_ids = names.iloc[i:i+50]["SongID"].tolist()
#         else:
#             song_ids = names.iloc[i:m]["SongID"].tolist()
#         i+=50
#         try:
#             info = sp.audio_features(song_ids)
#         except:
#             print ("Timeout occurred")
#         for j,val in enumerate(info):
#             if val!=None:
#                 info_line = [song_ids[j],val["danceability"],val["energy"],val["key"],val["loudness"],val["mode"],
#                             val["speechiness"],val["acousticness"],val["instrumentalness"],val["liveness"],val["valence"],val["tempo"]]
#                 info_line = [x if x!= None else np.nan for x in info_line]
#                 temp_info = pd.DataFrame([info_line],columns=[
#                     "SongID","danceability","energy","key","loudness","mode","speechiness",
#                 "acousticness","instrumentalness","liveness","valence","tempo"])
#                 final_df =final_df.append(temp_info,ignore_index = True)
#         print(i)
#     df_final=spark.createDataFrame(final_df)  
#     df_final.write.parquet("{}/{}".format(filepath, "final_music_4.parquet"))

def concat(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    idd = spark.read.parquet("{}/{}".format(filepath, "id_data.parquet"))
    idd.createOrReplaceTempView("idd")
    print(idd.count())
    final_df = spark.read.parquet("{}/{}".format(filepath, "final_music_5_2.parquet"))
    for i in range(1,11):
        temp_df = spark.read.parquet("{}/{}".format(filepath, "final_music_{}.parquet".format(i)))
        temp_df.createOrReplaceTempView("temp_df")
        final_df= final_df.union(temp_df)
    final_df.createOrReplaceTempView("final_df") 
    print(final_df.count())
    final = spark.sql("SELECT Artist, Gender, idd.SongID,Popularity,Year,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo FROM final_df LEFT JOIN idd on idd.SongId = final_df.SongId")
    print(final.count())
    # final.write.parquet("{}/{}".format(filepath, "final.parquet"))

def main():
    # song_info(filepath)    
    concat(filepath)
if __name__ == "__main__":
    main()

# check difference betwee inner annd right join




