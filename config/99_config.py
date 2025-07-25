# Databricks notebook source
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user') # user name associated with your account
user_sql = user.split('@')[0].replace('.', '_')

# COMMAND ----------

project_directory = '/home/{}/multi-touch-attribution'.format(user) # files will be written to this directory
catalog_name = user_sql
database_name = 'multi_touch_attribution'
dbutils.fs.mkdirs(f"{project_directory}/raw/")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"project_directory":project_directory,
                                  "database_name":database_name,
                                  "catalog_name":catalog_name,}))

# COMMAND ----------


