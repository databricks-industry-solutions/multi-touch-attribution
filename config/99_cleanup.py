# Databricks notebook source
# MAGIC %md
# MAGIC **Note:** Set reset to `"True"` and run this notebook if you would like to delete the artifacts created in this accelerator during a previous run.

# COMMAND ----------

# MAGIC %run ./99_utils 

# COMMAND ----------

reset = "False"

# COMMAND ----------

if reset == "True":
  reset_workspace("True")
else:
  pass
