from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat
import yfinance as yf
import boto3 as b3
import pandas
import os

#Initialize spark session
spark = SparkSession.builder.appName("market_data_clean_final").getOrCreate()

#Define the directory path to locate data files
filepath = "/home/ec2-user/project/datasets/"

#Get a list of all csv files in the folder
data_file = [file for file in os.listdir(filepath) if file.endswith('.csv')]

#Construct the file name with full path
for file in data_file:
    filename = os.path.join(filepath, file)

    #Extract the file name without extension
    ticker = os.path.splitext(file)[0]

    #Read the csv file as dataframe
    df = spark.read.csv(filename, header=True, inferSchema=True)

    #Add a column next to the first column
    col_name = "Ticker"
    df = df.withColumn(col_name, lit(ticker))

    #Convert PySpark dataframe to Pandas dataframe
    pandas_df = df.toPandas()

    #Write the file after transformation
    final_out = os.path.join(filepath, f"{ticker}_final.csv")
    pandas_df.to_csv(final_out,index=False)

#    df.show(5)

#Stop spark session
spark.stop()
