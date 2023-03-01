# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/multi-touch-attribution. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/multi-touch-attribution.

# COMMAND ----------

# MAGIC %md
# MAGIC <div >
# MAGIC   <img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/solution-accelerator-logo.png"; width="50%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC ### In this notebook you:
# MAGIC * Use `Databricks Autoloader` to import the ad impression and conversion data generated in the notebook `01_intro`.
# MAGIC * Write the data out in `Delta` format.
# MAGIC * Create a database and table for easy access and queryability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Environment
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Import libraries
# MAGIC   2. Run `utils` notebook to gain access to the function `get_params`
# MAGIC   3. `get_params` and store values in variables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.1: Import libraries

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.2: Run `utils` notebook to gain access to the function `get_params`
# MAGIC * `%run` is magic command provided within Databricks that enables you to run notebooks from within other notebooks.
# MAGIC * `get_params` is a helper function that returns a few parameters used throughout this solution accelerator. Usage of these parameters will be made explicit when used.

# COMMAND ----------

# MAGIC %run ./config/99_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.3: `get_params` and store values in variables
# MAGIC * Three of the parameters returned by `get_params` are used in this notebook. For convenience, we will store the values for these parameters in new variables.
# MAGIC   * **database_name:** the name of the database created in notebook `02_load_data`. The default value can be overridden in the notebook `99_config`.
# MAGIC   * **raw_data_path:** the path used when reading in the data generated in the notebook `01_intro`.
# MAGIC   * **bronze_tbl_path:** the path used in `02_load_data` to write out bronze-level data in delta format.

# COMMAND ----------

params = get_params()
database_name = params['database_name']
raw_data_path = params['raw_data_path']
bronze_tbl_path = params['bronze_tbl_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Data using Databricks Auto Loader
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Define the schema of the synthetic data generated in `01_load_data`
# MAGIC   2. Read the synthetic data into a dataframe using Auto Loader
# MAGIC   3. Add two new columns to the dataframe

# COMMAND ----------

# MAGIC %md 
# MAGIC But, what is Auto Loader?
# MAGIC * Auto Loader incrementally and efficiently loads new data files as they arrive in [S3](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) or [Azure Blog Storage](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader). This is enabled by providing a Structured Streaming source called `cloudFiles`. 
# MAGIC 
# MAGIC * Auto Loader internally keeps tracks of what files have been processed to provide exactly-once semantics, so you do not need to manage any state information yourself.
# MAGIC 
# MAGIC * Auto Loader supports two modes for detecting when new files arrive:
# MAGIC   
# MAGIC   * `Directory listing:` Identifies new files by parallel listing of the input directory. Quick to get started since no permission configurations are required. Suitable for scenarios where only a few files need to be streamed in on a regular basis.
# MAGIC   
# MAGIC   * `File Notification:` Uses AWS SNS and SQS services that subscribe to file events from the input directory. Auto Loader automatically sets up the AWS SNS and SQS services. File notification mode is more performant and scalable for large input directories.
# MAGIC   
# MAGIC * In this notebook, we use `Directory Listing` as that is the default mode for detecting when new files arrive.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.1: Define the schema of the synthetic data generated in `01_load_data`
# MAGIC * Auto loader requires you to specify your schema on read.
# MAGIC * Since we're working with a relatively small, and stable, data set, we are inferring the schema from the data itself. 

# COMMAND ----------

schema = spark.read.csv(raw_data_path,header=True).schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.2: Read the synthetic data into a dataframe using Auto Loader 

# COMMAND ----------

raw_data_df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.validateOptions", "false") \
            .option("cloudFiles.format", "csv") \
            .option("header", "true") \
            .option("cloudFiles.region", "us-west-2") \
            .option("cloudFiles.includeExistingFiles", "true") \
            .schema(schema) \
            .load(raw_data_path) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.3: Add two new columns to the dataframe

# COMMAND ----------

raw_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss"))\
              .withColumn("conversion", col("conversion").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Data to Delta Lake
# MAGIC 
# MAGIC In this section of the solution accelerator, we write our data out to [Delta Lake](https://delta.io/) and then create a table (and database) for easy access and queryability.
# MAGIC 
# MAGIC * Delta Lake is an open-source project that enables building a **Lakehouse architecture** on top of existing storage systems such as S3, ADLS, GCS, and HDFS.
# MAGIC    * Information on the **Lakehouse Architecture** can be found in this [paper](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) that was presented at [CIDR 2021](http://cidrdb.org/cidr2021/index.html) and in this [video](https://www.youtube.com/watch?v=RU2dXoVU8hY)
# MAGIC 
# MAGIC * Key features of Delta Lake include:
# MAGIC   * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC   * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC   * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC   * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC   * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC   * **Open Format**: Stored as Parquet format in blob storage.
# MAGIC   * **Audit History**: History of all the operations that happened in the table.
# MAGIC   * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC   

# COMMAND ----------

# MAGIC %md **Note:** For the purpose of this solution accelerator, we have set the trigger to `once=True`. In production, you may choose to keep Auto Loader running so that data is loaded continuously as new files arrive.

# COMMAND ----------

raw_data_df.writeStream.format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", bronze_tbl_path+"/checkpoint") \
  .start(bronze_tbl_path) \
  .awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Database

# COMMAND ----------

# Delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(database_name))

# Create database to house tables
_ = spark.sql('CREATE DATABASE {}'.format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create bronze-level table in Delta format
# MAGIC 
# MAGIC * **Note:** this step will produce an exception if it is run before writeStream in step 3 is initialized.
# MAGIC 
# MAGIC * The nomenclature of bronze, silver, and gold tables correspond with a commonly used data modeling approach known as multi-hop architecture. 
# MAGIC   * Additional information about this pattern can be found [here](https://databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html).

# COMMAND ----------

# Create bronze table
_ = spark.sql('''
  CREATE TABLE `{}`.bronze
  USING DELTA 
  LOCATION '{}'
  '''.format(database_name,bronze_tbl_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: View the bronze table
# MAGIC 
# MAGIC Using `spark.table` here enables use of Python. An alternative approach is to query the data directly using SQL. This will be shown in the `03_data_prep` notebook.

# COMMAND ----------

bronze_tbl = spark.table("{}.bronze".format(database_name))

# COMMAND ----------

display(bronze_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * In the next notebook, we will prepare this data so that it can be used for attribution modeling with Markov Chains.

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Matplotlib|Python Software Foundation (PSF) License |https://matplotlib.org/stable/users/license.html|https://github.com/matplotlib/matplotlib|
# MAGIC |Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
# MAGIC |Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Seaborn|BSD-3-Clause License|https://github.com/mwaskom/seaborn/blob/master/LICENSE|https://github.com/mwaskom/seaborn|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
