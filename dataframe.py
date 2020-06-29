import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
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

def get_ids(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    gender = spark.read.parquet("{}/{}".format(filepath, "gender_df.parquet"))
    gender.createOrReplaceTempView("gender")
    cid ="36b35ee75fec40c399220f9371d2e3b0" 
    secret = "c0ce447c51394e1198dc56fb787ee326"
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    gender_df = gender.select("*").toPandas()
    gender_df=pd.DataFrame(gender_df,columns=["name","gender"])
    id_df = pd.DataFrame(columns=["Artist","Gender","Song Id","Popularity"])
    for i,artist in enumerate(gender_df["name"]):
        artist_search = sp.search(q=artist, type='artist', limit=50,offset=0)
        for info in artist_search['artists']['items']:
            if artist in info["name"]:
                id_df.append([artist.encode("utf-8"),gender_df["gender"].iloc[i],info["id"],info["popularity"]],axis=0)
                print(id_df.iloc[-1,:])

def main():
    get_ids(filepath)    

if __name__ == "__main__":
    main()


