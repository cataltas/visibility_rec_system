import sys, getopt, random
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import percent_rank
from pyspark.sql import Window
from prepare_data import prepare
from prepare_data import train_val_test_split
from pyspark.mllib.tree import RandomForest


filepath = "hdfs:/user/ct2522"
data =prepare(filepath)  
train,val,test=train_val_test_split(data)
train_col=train.columns
train_col.remove("Popularity")
rf = RandomForest.trainClassifier(train)
rf.predict(val.drop("Popularity"))
