# Databricks notebook source
df = spark.sql("Select * from formula1_dev.bronze.drivers")

# COMMAND ----------

pip install pandas-profiling

# COMMAND ----------

import pandas as pd

# COMMAND ----------

from pandas_profiling.profile_report import ProfileReport

# COMMAND ----------

pip install --upgrade pandas_profiling

# COMMAND ----------

pip install bamboolib

# COMMAND ----------

import bamboolib as bam

# COMMAND ----------

df2 = df.toPandas

# COMMAND ----------


