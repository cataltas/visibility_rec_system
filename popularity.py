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
    id_df = pd.DataFrame()
    for i,artist in enumerate(gender_df["name"][0:10]):
        artist_search = sp.search(q=artist, type='artist', limit=50,offset=0)
        for info in artist_search['artists']['items']:
            if artist in info["name"]:
                temp_id=pd.DataFrame([[artist.encode("utf-8"),gender_df["gender"].iloc[i],info["id"],info["popularity"]]],columns=["Artist","Gender","Song Id","Popularity"])
                id_df = id_df.append(temp_id,ignore_index=True)
        print(i)
    print(id_df.info())
    df_id=spark.createDataFrame(id_df)  
    df_id.write.parquet("{}/{}".format(filepath, "id_df.parquet"))

def concat_files(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    final_id = spark.read.parquet("{}/{}".format(filepath, "id_df.parquet"))
    final_id.createOrReplaceTempView("final_id")
    for i in range(2,7):
        temp_id = spark.read.parquet("{}/id_df_{}.parquet".format(filepath, i))
        temp_id.createOrReplaceTempView("temp_id")
        final_id = final_id.union(temp_id)
    final_id.write.parquet("{}/{}".format(filepath, "final_id_df.parquet"))

def main():
    get_ids(filepath)    

if __name__ == "__main__":
    main()


