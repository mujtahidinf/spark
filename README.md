# spark
# Membuat Data frame dan create tabel di CDSW
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql import Window
from pyspark.sql.types import *
from datetime import date, timedelta, datetime, time
import logging
import seaborn as sns
import json
import pandas as pd
import re
import csv
from datetime import datetime
import math
import calendar
import os, time
from pyspark.sql.functions import col, countDistinct, regexp_replace
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import functions as F
import matplotlib.ticker as mticker
import numpy as np
os.environ['TZ']='Asia/Jakarta'
time.tzset()
time.strftime('%X %x %Z')

pd.options.display.max_columns = 999
pd.options.display.html.table_schema = True

pd.options.display.html.table_schema=True
pd.options.display.max_rows=999

# import library for visualization
import seaborn as sns
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

# set the style of the axes and the text color
plt.rcParams['figure.figsize'] = (12.0, 5.0)
sns.set_style("darkgrid", {"axes.facecolor": ".9"})
sns.set_context("paper")
sns.set_palette("bright")

spark = SparkSession \
    .builder \
    .appName("Hydin - DE") \
    .config('spark.dynamicAllocation.enabled', 'false') \
    .config('spark.executor.instances', '2') \
    .config('spark.executor.cores', '4') \
    .config('spark.executor.memory', '16g') \
    .config('spark.yarn.executor.memoryOverhead', '4g') \
    .enableHiveSupport() \
    .getOrCreate()

ddcust=spark.read.table("data_pribadi")
--membuat dataframe berdasarkan nama database yang akan digunakan

ddcust.createOrReplaceTempView("pribadi")
--membuat nama alias

df_pnjm_t1 = spark.sql("""
-- diisi oleh perintah sql select
""")
--ex select name from data_pribadi

hiveDB = "hdn"
-- nama Schema Tabel
hiveTable = "tbl_data_pribadi"
-- nama tabel pada hive
df_pnjm_t1\
    .write.format("parquet")\
    .mode("overwrite")\
    .saveAsTable("{}.{}".format(hiveDB,hiveTable))
