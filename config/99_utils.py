# Databricks notebook source
# Create reset_workspace widget
dbutils.widgets.dropdown("reset_workspace", "False", ["True", "False"])

# COMMAND ----------

def get_params():
  import json
  params = json.loads(dbutils.notebook.run('./config/99_config',timeout_seconds=60))
  project_directory = params['project_directory']
  database_name = params['database_name']
  data_gen_path = "/dbfs{}/raw/attribution_data.csv".format(project_directory)
  raw_data_path = "dbfs:{}/raw".format(project_directory)
  bronze_tbl_path = "dbfs:{}/bronze".format(project_directory)
  gold_user_journey_tbl_path = "dbfs:{}/gold_user_journey".format(project_directory)
  gold_attribution_tbl_path = "dbfs:{}/gold_attribution".format(project_directory)
  gold_ad_spend_tbl_path = "dbfs:{}/gold_ad_spend".format(project_directory)

  params = {"project_directory": project_directory,
            "database_name": database_name,
            "data_gen_path": data_gen_path,
            "raw_data_path": raw_data_path,
            "bronze_tbl_path": bronze_tbl_path,
            "gold_user_journey_tbl_path": gold_user_journey_tbl_path,
            "gold_attribution_tbl_path": gold_attribution_tbl_path,
            "gold_ad_spend_tbl_path": gold_ad_spend_tbl_path,
           }
  
  return params

# COMMAND ----------

def reset_workspace(reset_flag="False"):
  params = get_params()
  project_directory = params['project_directory']
  database_name = params['database_name']
  raw_data_path = params['raw_data_path']
  
  if reset_flag == "True":
    # Replace existing project directory with new one
    dbutils.fs.rm(project_directory, recurse=True)
    #dbutils.fs.mkdirs(project_directory)
    dbutils.fs.mkdirs(raw_data_path)
    
    # Replace existing database with new one
    spark.sql("DROP DATABASE IF EXISTS " +database_name+ " CASCADE")
    spark.sql("CREATE DATABASE " + database_name)
  elif dbutils.fs.ls(raw_data_path) == False:
    dbutils.fs.mkdirs(raw_data_path)
  return None

# COMMAND ----------


