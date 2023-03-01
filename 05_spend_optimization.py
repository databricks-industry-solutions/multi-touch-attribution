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
# MAGIC * Build a dashboard to visualize the current state of a given campaign and to inform marketing budget reallocation decisions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Environment
# MAGIC 
# MAGIC In this step, we will:
# MAGIC   1. Import libraries
# MAGIC   2. Run the `utils` notebook to gain access to the functions `get_params`
# MAGIC   3. `get_params` and store values in variables
# MAGIC   4. Set the current database so that it doesn't need to be manually specified each time it's used

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.1: Import libraries

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(font_scale = 1.4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.2: Run the `utils` notebook to gain access to the functions `get_params`
# MAGIC * `%run` is a magic command provided within Databricks that enables you to run notebooks from within other notebooks.
# MAGIC * `get_params` is a helper function that returns a few parameters used throughout this solution accelerator. Usage of these parameters will be explicit.

# COMMAND ----------

# MAGIC %run ./config/99_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3: `get_params` and store values in variables
# MAGIC 
# MAGIC * Three of the parameters returned by `get_params` are used in this notebook. For convenience, we will store the values for these parameters in new variables. 
# MAGIC 
# MAGIC   * **database_name:** the name of the database created in notebook `02_load_data`. The default value can be overridden in the notebook `99_config`
# MAGIC   * **gold_user_journey_tbl_path:** the path used in `03_load_data` to write out gold-level user journey data in delta format.
# MAGIC   * **gold_attribution_tbl_path:** the path used in `03_load_data` to write out gold-level attribution data in delta format.

# COMMAND ----------

params = get_params()
database_name = params['database_name']
gold_user_journey_tbl_path = params['gold_user_journey_tbl_path']
gold_attribution_tbl_path = params['gold_attribution_tbl_path']
gold_ad_spend_tbl_path = params['gold_ad_spend_tbl_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.4: Set the current database so that it doesn't need to be manually specified each time it's used
# MAGIC * Please note that this is a completely optional step. An alternative approach would be to use the syntax `database_name`.`table_name` when querying the respective tables. 

# COMMAND ----------

_ = spark.sql("use {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create and Populate Ad Spend Table
# MAGIC 1. Create ad spend table
# MAGIC 2. Create widget for specifying the ad spend for a given campaign
# MAGIC 3. Populate ad spend table with synthetic spend data
# MAGIC 4. View campaign ad spend details
# MAGIC 5. Explode struct into multiple rows

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.1: Create ad spend table

# COMMAND ----------

_ = spark.sql('''
  CREATE OR REPLACE TABLE gold_ad_spend (
    campaign_id STRING, 
    total_spend_in_dollars FLOAT, 
    channel_spend MAP<STRING, FLOAT>, 
    campaign_start_date TIMESTAMP)
  USING DELTA
  LOCATION '{}'
  '''.format(gold_ad_spend_tbl_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.2: Create widget for specifying the ad spend for a given campaign
# MAGIC * In practice, this data is typically captured directly from each marketing activation channel.
# MAGIC * After running this command, you will see a [widget](https://docs.databricks.com/notebooks/widgets.html) appear at the top of the notebook. Widgets are useful for making notebooks and dashboards interactive.
# MAGIC * The values passed to this widget include the name of the widget, the default value, and a label to be displayed next to the widget.

# COMMAND ----------

dbutils.widgets.text("adspend", "10000", "Campaign Budget in $")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.3: Populate ad spend table with synthetic ad spend data
# MAGIC * Note that the value for the widget is passed in using $adspend
# MAGIC * For illustration purposes, we have distributed spend uniformly across all marketing channels (20%)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE gold_ad_spend
# MAGIC VALUES ("3d65f7e92e81480cac52a20dfdf64d5b", $adspend,
# MAGIC           MAP('Social Network', .2,
# MAGIC               'Search Engine Marketing', .2,  
# MAGIC               'Google Display Network', .2, 
# MAGIC               'Affiliates', .2, 
# MAGIC               'Email', .2), 
# MAGIC          make_timestamp(2020, 5, 17, 0, 0, 0));

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.4: View campaign ad spend details
# MAGIC * The channel spend data currently exists as an array. We will explode these values into separate columns in the next step

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_ad_spend

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.5: Explode struct into multiple rows

# COMMAND ----------

ad_spend_df = spark.sql('select explode(channel_spend) as (channel, pct_spend), \
                         round(total_spend_in_dollars * pct_spend, 2) as dollar_spend \
                         from gold_ad_spend')

ad_spend_df.createOrReplaceTempView("exploded_gold_ad_spend")
display(ad_spend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: View Campaign Performance
# MAGIC In this section, we will create the following charts:
# MAGIC 1. Base conversion rate
# MAGIC 2. Conversions by date
# MAGIC 3. Attribution by model type
# MAGIC 4. Cost per acquisition

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.1: Base conversion rate

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE base_conversion_rate
# MAGIC USING DELTA AS
# MAGIC SELECT count(*) as count,
# MAGIC   CASE 
# MAGIC     WHEN conversion == 0 
# MAGIC     THEN 'Impression'
# MAGIC     ELSE 'Conversion'
# MAGIC   END AS interaction_type
# MAGIC FROM
# MAGIC   gold_user_journey
# MAGIC GROUP BY
# MAGIC   conversion;

# COMMAND ----------

base_conversion_rate_pd = spark.table("base_conversion_rate").toPandas()

pie, ax = plt.subplots(figsize=[20,9])
labels = base_conversion_rate_pd['interaction_type']
plt.pie(x=base_conversion_rate_pd['count'], autopct="%.1f%%", explode=[0.05]*2, labels=labels, pctdistance=0.5)
plt.title("Base Conversion Rate");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.2: Conversions by date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE conversions_by_date 
# MAGIC USING DELTA AS
# MAGIC SELECT count(*) AS count,
# MAGIC   'Conversion' AS interaction_type,
# MAGIC   date(time) AS date
# MAGIC FROM bronze
# MAGIC WHERE conversion = 1
# MAGIC GROUP BY date
# MAGIC ORDER BY date;

# COMMAND ----------

conversions_by_date_pd = spark.table("conversions_by_date").toPandas()

plt.figure(figsize=(20,9))
pt = sns.lineplot(x='date',y='count',data=conversions_by_date_pd)

pt.tick_params(labelsize=20)
pt.set_xlabel('Date')
pt.set_ylabel('Number of Conversions')
plt.title("Conversions by Date");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.3: Attribution by model type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE attribution_by_model_type 
# MAGIC USING DELTA AS
# MAGIC SELECT attribution_model, channel, round(attribution_percent * (
# MAGIC     SELECT count(*) FROM gold_user_journey WHERE conversion = 1)) AS conversions_attributed
# MAGIC FROM gold_attribution;

# COMMAND ----------

attribution_by_model_type_pd = spark.table("attribution_by_model_type").toPandas()

pt = sns.catplot(x='channel',y='conversions_attributed',hue='attribution_model',data=attribution_by_model_type_pd, kind='bar', aspect=4, legend=True)
pt.fig.set_figwidth(20)
pt.fig.set_figheight(9)

plt.tick_params(labelsize=15)
plt.ylabel("Number of Conversions")
plt.xlabel("Channels")
plt.title("Channel Performance");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.4: Cost per acquisition

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE cpa_summary 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   spending.channel,
# MAGIC   spending.dollar_spend,
# MAGIC   attribution_count.attribution_model,
# MAGIC   attribution_count.conversions_attributed,
# MAGIC   round(spending.dollar_spend / attribution_count.conversions_attributed,2) AS CPA_in_Dollars
# MAGIC FROM
# MAGIC   (SELECT explode(channel_spend) AS (channel, spend),
# MAGIC    round(total_spend_in_dollars * spend, 2) AS dollar_spend
# MAGIC    FROM gold_ad_spend) AS spending
# MAGIC JOIN
# MAGIC   (SELECT attribution_model, channel, round(attribution_percent * (
# MAGIC       SELECT count(*) FROM gold_user_journey WHERE conversion = 1)) AS conversions_attributed
# MAGIC    FROM gold_attribution) AS attribution_count
# MAGIC ON spending.channel = attribution_count.channel;

# COMMAND ----------

cpa_summary_pd = spark.table("cpa_summary").toPandas()

pt = sns.catplot(x='channel', y='CPA_in_Dollars',hue='attribution_model',data=cpa_summary_pd, kind='bar', aspect=4, ci=None)
plt.title("Cost of Acquisition by Channel")
pt.fig.set_figwidth(20)
pt.fig.set_figheight(9)

plt.tick_params(labelsize=15)
plt.ylabel("CPA in $")
plt.xlabel("Channels")
plt.title("Channel Cost per Acquisition");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Budget Allocation Optimization.
# MAGIC Now that we have assigned credit to our marketing channels using Markov Chains, we can take a data-driven approach for budget allocation.
# MAGIC 
# MAGIC * One KPI we can take a look at is Return on Ad Spend (ROAS).
# MAGIC * In the ecommerce world, ROAS is calculated as: <br>
# MAGIC ``ROAS = Revenue $ from marketing/ Advertising $ spent``
# MAGIC 
# MAGIC In our example, instead of working with exact $ values, we will divide the % of conversion attributed to a channel by the % of total adspend allocated to that channel.
# MAGIC * ``ROAS = CHANNEL CONVERSION WEIGHT / CHANNEL BUDGET WEIGHT``
# MAGIC   * ROAS value > 1 signifies that the channel has been allocated less budget than warranted by its conversion rate.
# MAGIC   * ROAS value < 1 signifies that the channel has been allocated more budget than warranted by its conversion rate.
# MAGIC   * ROAS value = 1 signifies and optimized budget allocation. 
# MAGIC 
# MAGIC From ROAS, we can calculate the Proposed Budget for each channel <br>
# MAGIC * ``Proposed budget =  Current budget X ROAS``
# MAGIC <br>
# MAGIC 
# MAGIC To calculate ROAS we will join the following Delta Tables:
# MAGIC * **gold_attribution:** This table contains the calculated attribution % per channel based on different attribution models.
# MAGIC * **exploded_gold_ad_spend:** This table contains the current budget allocated per channel. The column pct_spend documents the % of the total budget that has been allocated to a given channel. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM exploded_gold_ad_spend;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_attribution;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE spend_optimization_view 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   a.channel,
# MAGIC   a.pct_spend,
# MAGIC   b.attribution_percent,
# MAGIC   b.attribution_percent / a.pct_spend as ROAS,
# MAGIC   a.dollar_spend,
# MAGIC   round(
# MAGIC     (b.attribution_percent / a.pct_spend) * a.dollar_spend,
# MAGIC     2
# MAGIC   ) as proposed_dollar_spend
# MAGIC FROM
# MAGIC   exploded_gold_ad_spend a
# MAGIC   JOIN gold_attribution b on a.channel = b.channel
# MAGIC   and attribution_model = 'markov_chain';
# MAGIC   
# MAGIC CREATE
# MAGIC OR REPLACE TABLE spend_optimization_final 
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   channel,
# MAGIC   'current_spending' AS spending,
# MAGIC   dollar_spend as budget
# MAGIC  FROM exploded_gold_ad_spend
# MAGIC UNION
# MAGIC SELECT
# MAGIC   channel,
# MAGIC   'proposed_spending' AS spending,
# MAGIC   proposed_dollar_spend as budget
# MAGIC FROM
# MAGIC   spend_optimization_view;  

# COMMAND ----------

spend_optimization_final_pd = spark.table("spend_optimization_final").toPandas()

pt = sns.catplot(x='channel', y='budget', hue='spending', data=spend_optimization_final_pd, kind='bar', aspect=4, ci=None)

plt.tick_params(labelsize=15)
pt.fig.set_figwidth(20)
pt.fig.set_figheight(9)
plt.title("Spend Optimization per Channel")
plt.ylabel("Budget in $")
plt.xlabel("Channels")

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
