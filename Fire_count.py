# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 20:18:37 2022

@author: Venkat Bhaskar
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark=SparkSession.builder.appName("SF_fire").getOrCreate()
#schema
fire_schema=StructType([StructField('IncidentNumber', IntegerType(), True),
                        StructField('Exposure Number', IntegerType(), True),
                        StructField('ID', StringType(), True),
                        StructField('Address', StringType(), True),
                        StructField('Incident Date',DateType(), True),
                        StructField('Call Number', StringType(), True),
                        StructField('Alarm DtTm', DateType(), True),
                        StructField('Arrival DtTm', DateType(), True),
                        StructField('Close DtTm', DateType(), True),
                        StructField('City', StringType(), True), 
                        StructField('Zipcode', IntegerType(), True), 
                        StructField('Battalion', StringType(), True), 
                        StructField('StationArea', StringType(), True), 
                        StructField('Box', StringType(), True),
                        StructField('Suppression Units', StringType(), True),
                        StructField('Supression Personnel', StringType(), True),
                        StructField('EMS Units', StringType(), True),
                        StructField('EMS Personnel', StringType(), True),
                        StructField('Other Personnel', StringType(), True),
                        StructField('First Unit On Scene', StringType(), True),
                        StructField('Estimated Property Loss', StringType(), True),
                        StructField('Estimated Current Loss', StringType(), True),
                        StructField('Fire Fatalities', IntegerType(), True),
                        StructField('Fire Injuries', IntegerType(), True),
                        StructField('Civilian Fatalities', IntegerType(), True),
                        StructField('Civillian Injuries', IntegerType(), True),
                        StructField('Number of Alarms', IntegerType(), True),
                        StructField('Primary Situation', StringType(), True), 
                        StructField('Mutual Aid', StringType(), True), 
                        StructField('Action Taken Primary', StringType(), True), 
                        StructField('Action Taken Secondary', StringType(), True), 
                        StructField('Action Taken Other', StringType(), True), 
                        StructField('Detected Alerted Occupants', StringType(), True), 
                        StructField('Property Use', StringType(), True), 
                        StructField('Area of Fire Origin', StringType(), True), 
                        StructField('Ignition Cause Primary', StringType(), True), 
                        StructField('Ignition Factor Primary', StringType(), True), 
                        StructField('Ignition Factor Secondary', StringType(), True), 
                        StructField('Heat Source', StringType(), True), 
                        StructField('Item First Ignited', StringType(), True), 
                        StructField('Human Factors Associated with Ignition', StringType(), True), 
                        StructField('Structure Type', StringType(), True),
                        StructField('Structure Status', StringType(), True),
                        StructField('Floor of Fire Origin', StringType(), True), 
                        StructField('Fire Spread', StringType(), True), 
                        StructField('No Flame Spread', StringType(), True), 
                        StructField('Number of floors with minimum damage', IntegerType(), True), 
                        StructField('Number of floors with significant damage', IntegerType(), True), 
                        StructField('Number of floors with heavy damage', IntegerType(), True), 
                        StructField('Number of floors with Extreme damage', IntegerType(), True), 
                        StructField('Detectors Present', IntegerType(), True), 
                        StructField('Detector Type', StringType(), True), 
                        StructField('Detector Operation', StringType(), True), 
                        StructField('Detector Effectiveness', StringType(), True), 
                        StructField('Detector Failure Reason', StringType(), True), 
                        StructField('Automatic Extinguishing System Present', StringType(), True), 
                        StructField('Automatic Extinguishing System Type', StringType(), True), 
                        StructField('Automatic Extinguishing System Performance', StringType(), True), 
                        StructField('Automatic Extinguishing System Failure Reason', StringType(), True), 
                        StructField('Number of Sprinkler Heads Operating', IntegerType(), True), 
                        StructField('Supervisor District', StringType(), True),
                        StructField('Neighborhood', StringType(), True),
                        StructField('Location', StringType(), True)])

#Reading the csv file and creating a DataFrame
sf_fire_file="file:///SparkCourse/Fire_incidents.csv"
fire_df=spark.read.schema(fire_schema).option("header","true").csv(sf_fire_file)

#Transformations
few_fire_df = (fire_df.select("IncidentNumber","Arrival DtTm","Primary Situation").where(F.col("Primary Situation")!="Medical Incident"))
few_fire_df.show(5,truncate=False)
fire_df.select("Primary Situation").where(F.col("Primary Situation").isNotNull()).agg(countDistinct("Primary Situation").alias("Distinct Primary Situations")).show()
fire_df.show(5,truncate=False)
fire_df.select("Primary Situation").where(F.col("Primary Situation").isNotNull()).distinct().show(5,truncate=False)
new_fire_df=fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")