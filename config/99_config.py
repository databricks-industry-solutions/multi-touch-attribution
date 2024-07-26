# Databricks notebook source
# user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user') # user name associated with your account
user = 'chad.lorenz@amperity.com'
user_sql = user.split('@')[0].replace('.', '_')

# COMMAND ----------

project_directory = '/home/{}/multi-touch-attribution'.format(user) # files will be written to this directory
database_name = f'multi_touch_attribution_{user_sql}' # tables will be stored in this database
dbutils.fs.mkdirs(f"{project_directory}/raw/")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"project_directory":project_directory,"database_name":database_name}))
