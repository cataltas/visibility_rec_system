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
def main():
    get_ids(filepath)    

if __name__ == "__main__":
    main()

# Total: 321 324 artists, 69 825 females, 251 499 males
# Total songs: 1 335 517 songs, 250 050 females, 1 085 467 males
# Songs >=80: 1 576 total, 269 females, 1307 males
# Songs <10: 920 487 total, 166 823 females, 753 664 males
# 0-100 000 pop_one
# 100-200 pop_two
# 200-300 pop_three
# 300-400 pop_four
# 400-500 pop_five 
# 500: pop_last 

# POP ONE: issue at 46882 or so
# POP THREE: 1 126 637 songs, 51 417 artists
# POP FOUR: 41 365 artists, 773 752 songs, 605472<10, 290>80, all one gender
# POP FIVE: 743 817 songs, 40 871 artists, 
# POP SIX: 40396 


# TODO: check stamps of where is problem and print for both issues 

