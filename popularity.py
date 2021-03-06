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
    for i,artist in enumerate(gender_df["name"].iloc[500000:]):
        try:
            artist_search = sp.search(q=artist, type='track', limit=50,offset=0)
        except:
            print ("Timeout occurred")
        for info in artist_search["tracks"]["items"]:
            count =0
            for j in info["artists"]:
                if artist in j["name"]:
                    count+=1
            if count>0:
                info_list = [artist,gender_df["gender"].iloc[i],info["id"],info["popularity"]]
                date = info["album"]['release_date'].split("-")[0]
                info_list.append(date)
                temp_id = pd.DataFrame([info_list],columns=["Artist","Gender","SongId","Popularity","Year"])
                id_df = id_df.append(temp_id,ignore_index=True)
        print(i)
    df_id=spark.createDataFrame(id_df)  
    df_id.write.parquet("{}/{}".format(filepath, "id_data_6.parquet"))

# Function to concatenate and fix mistake in gender previously made 
def concat(file_path,sparkSession=None):
    spark = sparkSession or newSparkSession()
    gen = spark.read.parquet("{}/{}".format(filepath, "gender_df.parquet"))
    gen.createOrReplaceTempView("gen")
    final_df = spark.read.parquet("{}/{}".format(filepath, "id_data_1.parquet"))
    for i in range(2,7):
        temp_df = spark.read.parquet("{}/{}".format(filepath, "id_data_{}.parquet".format(i)))
        temp_df.createOrReplaceTempView("temp_df")
        final_df= final_df.union(temp_df)
    final_df.createOrReplaceTempView("final_df") 
    fix_gender = spark.sql("SELECT name AS Artist, gen.gender AS Gender, SongID, Popularity,Year FROM final_df INNER JOIN gen on gen.name = final_df.Artist")
    fix_gender.write.parquet("{}/{}".format(filepath, "id_data.parquet"))
        
def main():
    get_ids(filepath)  
    concat(filepath)

if __name__ == "__main__":
    main()

# POP ONE: 1 080 537 songs, 48 739 artists
# POP TWO: 1 346 857 songs, 57 763 artists
# POP THREE: 1 126 637 songs, 51 417 artists
# POP FOUR: 41 365 artists, 773 752 songs
# POP FIVE: 743 817 songs, 40 871 artists
# POP SIX: 919 709 songs, 55 219 artists
# TOTAL: 5 991 309 songs, 295 374 artists, 66 122 female, 229 252 male
# 2 052 >80 popularity (338 female,1 714 male), 4 491 777 <10 (1 010 251 female,4 491 777 male, 3 080 507 have zero
# Year: 0000 to 2020, 717 years 0000
# TODO: see if ratio of women proportional to popularity, if so problem more out of my hands







