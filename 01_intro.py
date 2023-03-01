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
# MAGIC Behind the growth of every consumer-facing product is the acquisition and retention of an engaged user base. When it comes to acquisition, the goal is to attract high quality users as cost effectively as possible. With marketing dollars dispersed across a wide array of campaigns, channels, and creatives, however, measuring effectiveness is a challenge. In other words, it's difficult to know how to assign credit where credit is due. Enter multi-touch attribution. With multi-touch attribution, credit can be assigned in a variety of ways, but at a high-level, it's typically done using one of two methods: `heuristic` or `data-driven`.
# MAGIC 
# MAGIC * Broadly speaking, heuristic methods are rule-based and consist of both `single-touch` and `multi-touch` approaches. Single-touch methods, such as `first-touch` and `last-touch`, assign credit to the first channel, or the last channel, associated with a conversion. Multi-touch methods, such as `linear` and `time-decay`, assign credit to multiple channels associated with a conversion. In the case of linear, credit is assigned uniformly across all channels, whereas for time-decay, an increasing amount of credit is assigned to the channels that appear closer in time to the conversion event.
# MAGIC 
# MAGIC * In contrast to heuristic methods, data-driven methods determine assignment using probabilities and statistics. Examples of data-driven methods include `Markov Chains` and `SHAP`. In this series of notebooks, we cover the use of Markov Chains and include a comparison to a few heuristic methods.

# COMMAND ----------

# MAGIC %md
# MAGIC ## About This Series of Notebooks
# MAGIC 
# MAGIC * This series of notebooks is intended to help you use multi-touch attribution to optimize your marketing spend.
# MAGIC 
# MAGIC * In support of this goal, we will:
# MAGIC  * Generate synthetic ad impression and conversion data.
# MAGIC  * Create a streaming pipeline for processing ad impression and conversion data in near real-time.
# MAGIC  * Create a batch pipeline for managing summary tables used for reporting, ad hoc queries, and decision support.
# MAGIC  * Calculate channel attribution using Markov Chains.
# MAGIC  * Create a dashboard for monitoring campaign performance and optimizing marketing spend.

# COMMAND ----------

# MAGIC %md
# MAGIC ## About the Data
# MAGIC * This series of notebooks uses a synthetic dataset that is designed to reflect what is used in practice. This data includes the following columns:
# MAGIC <table>
# MAGIC    <thead>
# MAGIC       <tr>
# MAGIC          <th>Column Name</th>
# MAGIC          <th>Description</th>
# MAGIC          <th>Data Type</th>
# MAGIC       </tr>
# MAGIC    </thead>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td>uid</td>
# MAGIC          <td>A unique identifier for each individual customer.</td>
# MAGIC          <td>String</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>time</td>
# MAGIC          <td>The date and time of when a customer interacted with an impression.</td>
# MAGIC          <td>Timestamp</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td>interaction</td>
# MAGIC          <td>Denotes whether the interaction was an impression or conversion.</td>
# MAGIC          <td>String</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>channel</td>
# MAGIC          <td>Ad Channel</td>
# MAGIC          <td>String</td>
# MAGIC       </tr>
# MAGIC      <tr>
# MAGIC          <td>conversion</td>
# MAGIC          <td>Conversion value</td>
# MAGIC          <td>Int</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC 
# MAGIC * In the following sections, you will generate this synthetic dataset and then process it using Structured Streaming. You will then apply additional transformations so that it is suitable to use with Markov Chains.
# MAGIC 
# MAGIC * **Note:** Default settings are used to generate this data set. After working through this series of notebooks for the first time, you may want to customize these settings for additional exploration. Please note that if you do so, commentary in the notebooks may not line up with the newly generated data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Environment
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Import libraries
# MAGIC   2. Run the `99_utils` notebook to gain access to the function `get_params`
# MAGIC   3. View the parameters returned by `get_params`
# MAGIC   4. `get_params` and store values in variables  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.1: Import libraries

# COMMAND ----------

import json
import pandas as pd
import uuid
import random
from random import randrange
from datetime import datetime, timedelta 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.2: Run the `99_utils` notebook to gain access to the function `get_params`
# MAGIC * `%run` is a magic command provided within Databricks that enables you to run notebooks from within other notebooks.
# MAGIC * `get_params` is a helper function that returns parameters used throughout this solution accelerator. Usage of these parameters will be made explicit when used.

# COMMAND ----------

# MAGIC %run ./config/99_utils 

# COMMAND ----------

reset_workspace(reset_flag="True")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.3: View the parameters returned by `get_params`
# MAGIC * As seen below, the parameters returned by `get_params` are mostly file paths. These file paths are derived from `project_directory`.
# MAGIC * The default value for `project_directory` can be overridden in the notebook `99_config`. Likewise, `database_name` can be overridden in `99_config` as well.

# COMMAND ----------

get_params()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.4: `get_params` and store values in variables
# MAGIC * Three of the parameters returned by `get_params` are used in this notebook. For convenience, we will store the values for these parameters in new variables.
# MAGIC   * **project_directory:** the directory used to store the files created in this solution accelerator. The default value can be overridden in the notebook `99_config`.
# MAGIC   * **data_gen_path:** the synthetic dataset generated in this notebook will be written to this file path. This file path is derived from **project directory**
# MAGIC   * **raw_data_path:** the path used when reading in the data generated in this notebook.

# COMMAND ----------

params = get_params()
project_directory = params['project_directory']
data_gen_path = params['data_gen_path']
raw_data_path = params['raw_data_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate the Data
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Define the functions that will be used to generate the synthetic data
# MAGIC   2. Call `data_gen` to generate the synthetic data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.2: Define the functions that will be used to generate the synthetic data

# COMMAND ----------

def _get_random_datetime(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def data_gen(data_gen_path):
  """
  This function will generate and write synthetic data for multi-touch 
  attribution modeling
  """
  
  # Open file and write out header row
  my_file = open(data_gen_path, "w")
  my_file.write("uid,time,interaction,channel,conversion\n")
  
  # Specify the number of unique user IDs that data will be generated for
  unique_id_count = 500000
  
  # Specify the desired conversion rate for the full data set
  total_conversion_rate_for_campaign = .14
  
  # Specify the desired channels and channel-level conversion rates
  base_conversion_rate_per_channel = {'Social Network':.3, 
                                      'Search Engine Marketing':.2, 
                                      'Google Display Network':.1, 
                                      'Affiliates':.39, 
                                      'Email':0.01}
  
  
  channel_list = list(base_conversion_rate_per_channel.keys())
  
  base_conversion_weight = tuple(base_conversion_rate_per_channel.values())
  intermediate_channel_weight = (20, 30, 15, 30, 5)
  channel_probability_weights = (20, 20, 20, 20, 20)
  
  # Generate list of random user IDs
  uid_list = []
  
  for _ in range(unique_id_count):
    uid_list.append(str(uuid.uuid4()).replace('-',''))
  
  # Generate data / user journey for each unique user ID
  for uid in uid_list:
      user_journey_end = random.choices(['impression', 'conversion'], 
                                        (1-total_conversion_rate_for_campaign, total_conversion_rate_for_campaign), k=1)[0]
      
      steps_in_customer_journey = random.choice(range(1,10))
      
      d1 = datetime.strptime('5/17/2020 1:30 PM', '%m/%d/%Y %I:%M %p')
      d2 = datetime.strptime('6/10/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

      final_channel = random.choices(channel_list, base_conversion_weight , k=1)[0]
      
      for i in range(steps_in_customer_journey):
        next_step_in_user_journey = random.choices(channel_list, weights=intermediate_channel_weight, k=1)[0] 
        time = str(_get_random_datetime(d1, d2))
        d1 = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        
        if user_journey_end == 'conversion' and i == (steps_in_customer_journey-1):
          my_file.write(uid+','+time+',conversion,'+final_channel+',1\n')
        else:
          my_file.write(uid+','+time+',impression,'+next_step_in_user_journey+',0\n')
  
  # Close file
  my_file.close() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.2: Call `data_gen` to generate the synthetic data

# COMMAND ----------

data_gen(data_gen_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: View the Generated Data

# COMMAND ----------

display(spark.read.format('csv').option('header','true').load(raw_data_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * In the next notebook, we will load the data we generated here into [Delta](https://docs.databricks.com/delta/delta-intro.html) tables.

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
