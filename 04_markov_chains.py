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
# MAGIC 
# MAGIC ## Overview
# MAGIC ### In this notebook you:
# MAGIC * Review how markov chain attribution models work
# MAGIC * Construct a transition probability matrix
# MAGIC * Calculate total conversion probability
# MAGIC * Use the removal effect to calculate attribution
# MAGIC * Compare channel performance across methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intro to Multi-Touch Attribution with Markov Chains

# COMMAND ----------

# MAGIC %md
# MAGIC **Overview**
# MAGIC * Heuristic-based attribution methods like first-touch, last-touch, and linear are relatively easy to implement but are less accurate than data-driven methods. With marketing dollars at stake, data-driven methods are highly recommended.
# MAGIC 
# MAGIC * There are three steps to take when using Markov Chains to calculate attribution:
# MAGIC   * Step 1: Construct a transition probability matrix
# MAGIC   * Step 2: Calculate the total conversion probability
# MAGIC   * Step 3: Use the removal effect to calculate attribution
# MAGIC   
# MAGIC * As the name suggests, a transition probability matrix is a matrix that contains the probabilities associated with moving from one state to another state. This is calculated using the data from all available customer journeys. With this matrix in place, we can then easily calculate the total conversion probability, which represents, on average, the likelihood that a given user will experience a conversion event. Lastly, we use the total conversion probability as an input for calculating the removal effect for each channel. The way that the removal effect is calculated is best illustrated with an example.
# MAGIC 
# MAGIC **An Example**
# MAGIC 
# MAGIC In the image below, we have a transition probability graph that shows the probability of going from one state to another state. In the context of a customer journey, states can be non-terminal (viewing an impression on a given channel) or terminal (conversion, no conversion).
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/multi-touch-attribution/mta-dag-1.png"; width="60%">
# MAGIC </div>
# MAGIC 
# MAGIC This image, which is simply a visual representation of a transition probability matrix, can be used to calculate the total conversion probability. The total conversion probability can be calculated by summing the probability of every path that leads to a conversion. For example, in the image above, we have 5 paths that lead to conversion. The paths and conversion probabilities are: 
# MAGIC 
# MAGIC | Path | Conversion Probability |
# MAGIC |---|---|
# MAGIC | State --> Facebook --> Conversion| 0.2 x 0.8|
# MAGIC | Start --> Facebook --> Email --> Conversion | 0.2 x 0.2 x 0.1 | 
# MAGIC | Start --> Google Display / Search --> Conversion | 0.8 x 0.6 | 
# MAGIC | Start --> Google Display / Search --> Facebook / Social --> Conversion | 0.8 x 0.4 x 0.8 |
# MAGIC | Start --> Google Display / Search --> Facebook / Social -- Email --> Conversion | 0.8 x 0.4 x 0.2 x 0.1 |
# MAGIC 
# MAGIC Therefore, the total probability of conversion is `0.90`:
# MAGIC 
# MAGIC ```P(Conversion) = (0.2 X 0.8) + (0.2 X 0.2 X 0.1) + (0.8 X 0.6) + (0.8 X 0.4 X 0.8) + (0.8 X 0.4 X 0.2 X 0.1)  = 0.90```
# MAGIC 
# MAGIC Now, let's calculate the removal effect for one of our channels: Facebook/Social. For this, we will set the conversion for Facebook/Social to 0% and then recalculate the total conversion probability. Now we have `0.48`.
# MAGIC 
# MAGIC ```P(Conversion) = (0.2 X 0.0) + (0.2 X 0.0 X 0.1) + (0.8 X 0.6) + (0.8 X 0.4 X 0) +(0.8 X 0.4 X 0.0 X 0.1)  = 0.48```
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/multi-touch-attribution/mta-dag-2.png"; width="60%">
# MAGIC </div>
# MAGIC 
# MAGIC With these two probabilities, we can now calculate the removal effect for Facebook/Social. The removal effect can be calculated as the difference between the total conversion probability (with all channels) and the conversion probability when the conversion for Facebook/Social is set to 0%.
# MAGIC 
# MAGIC ```Removal Effect(Facebook/ Social media) = 0.90 - 0.48 = 0.42```
# MAGIC 
# MAGIC Similarly, we can calculate the removal effect for each of the other channels and calculate attribution accordingly.
# MAGIC 
# MAGIC An excellent visual explanation of Markov Chains is available in this [article](https://setosa.io/ev/markov-chains/).

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Configure the Environment
# MAGIC 
# MAGIC In this step, we will:
# MAGIC 1. Import libraries
# MAGIC 2. Run the utils notebook to gain access to the get_params function
# MAGIC 3. get_params and store the relevant values in variables
# MAGIC 4. Set the current database so that it doesn't need to be manually specified each time it's used

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.1: Import libraries

# COMMAND ----------

from pyspark.sql.types import StringType, ArrayType
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.2: Run the `utils` notebook to gain access to the function `get_params`
# MAGIC * `%run` is a magic command provided within Databricks that enables you to run notebooks from within other notebooks.
# MAGIC * `get_params` is a helper function that returns a few parameters used throughout this solution accelerator. Usage of these parameters will be explicit.

# COMMAND ----------

# MAGIC %run ./config/99_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.3: `get_params` and store values in variables
# MAGIC * Two of the parameters returned by `get_params` are used in this notebook. For convenience, we will store the values for these parameters in new variables. 
# MAGIC   * **project_directory:** the directory used to store the files created in this solution accelerator. The default value can be overridden in the notebook 99_config.
# MAGIC   * **database_name:** the name of the database created in notebook `02_load_data`. The default value can be overridden in the notebook `99_config`

# COMMAND ----------

params = get_params()
project_directory = params['project_directory']
database_name = params['database_name']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1.4: Set the current database so that it doesn't need to be manually specified each time it's used.
# MAGIC * Please note that this is a completely optional step. An alternative approach would be to use the syntax `database_name`.`table_name` when querying the respective tables.

# COMMAND ----------

_ = spark.sql("use {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Construct the Transition Probability Matrix
# MAGIC 
# MAGIC As discussed above, the transition probability matrix contains the probabilities associated with moving from one state to another state. This is calculated using the data from all customer journeys.
# MAGIC 
# MAGIC In this step, we will:
# MAGIC 1. Define a user-defined function (UDF), `get_transition_array`, that takes a customer journey and enumerates each of the corresponding channel transitions
# MAGIC 2. Register the `get_transition_array` udf as a Spark UDF so that it can be utilized in Spark SQL
# MAGIC 3. Use `get_transition_array` to enumerate all channel transitions in a customer's journey
# MAGIC 4. Construct the transition probability matrix
# MAGIC 5. Validate that the state transition probabilities are calculated correctly
# MAGIC 6. Display the transition probability matrix

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2.1: Define a user-defined function (UDF) that takes a customer journey and enumerates each of the corresponding channel transitions

# COMMAND ----------

 def get_transition_array(path):
  '''
    This function takes as input a user journey (string) where each state transition is marked by a >. 
    The output is an array that has an entry for each individual state transition.
  '''
  state_transition_array = path.split(">")
  initial_state = state_transition_array[0]
  
  state_transitions = []
  for state in state_transition_array[1:]:
    state_transitions.append(initial_state.strip()+' > '+state.strip())
    initial_state =  state
  
  return state_transitions

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2.2: Register the `get_transition_array` udf as a Spark UDF so that it can be utilized in Spark SQL
# MAGIC * Note: this is an optional step that enables cross-language support.

# COMMAND ----------

spark.udf.register("get_transition_array", get_transition_array, ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2.3: Use the `get_transition_array` to enumerate all channel transitions in a customer's journey

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW markov_state_transitions AS
# MAGIC SELECT path,
# MAGIC   explode(get_transition_array(path)) as transition,
# MAGIC   1 AS cnt
# MAGIC FROM
# MAGIC   gold_user_journey

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from markov_state_transitions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.4: Construct the transition probability matrix

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW transition_matrix AS
# MAGIC SELECT
# MAGIC   left_table.start_state,
# MAGIC   left_table.end_state,
# MAGIC   left_table.total_transitions / right_table.total_state_transitions_initiated_from_start_state AS transition_probability
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       transition,
# MAGIC       sum(cnt) total_transitions,
# MAGIC       trim(SPLIT(transition, '>') [0]) start_state,
# MAGIC       trim(SPLIT(transition, '>') [1]) end_state
# MAGIC     FROM
# MAGIC       markov_state_transitions
# MAGIC     GROUP BY
# MAGIC       transition
# MAGIC     ORDER BY
# MAGIC       transition
# MAGIC   ) left_table
# MAGIC   JOIN (
# MAGIC     SELECT
# MAGIC       a.start_state,
# MAGIC       sum(a.cnt) total_state_transitions_initiated_from_start_state
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           trim(SPLIT(transition, '>') [0]) start_state,
# MAGIC           cnt
# MAGIC         FROM
# MAGIC           markov_state_transitions
# MAGIC       ) AS a
# MAGIC     GROUP BY
# MAGIC       a.start_state
# MAGIC   ) right_table ON left_table.start_state = right_table.start_state
# MAGIC ORDER BY
# MAGIC   end_state DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.5: Validate that the state transition probabilities are calculated correctly
# MAGIC * Sum of all the outgoing probabilities (edges) from any channel (state in the graph) should sum to 100%. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT start_state, round(sum(transition_probability),2) as transition_probability_sum 
# MAGIC FROM transition_matrix
# MAGIC GROUP BY start_state

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2.6: Display the transition probability matrix

# COMMAND ----------

transition_matrix_pd = spark.table('transition_matrix').toPandas()
transition_matrix_pivot = transition_matrix_pd.pivot(index='start_state',columns='end_state',values='transition_probability')

plt.figure(figsize=(10,5))
sns.set(font_scale=1.4)
sns.heatmap(transition_matrix_pivot,cmap='Blues',vmax=0.25,annot=True)

# COMMAND ----------

# MAGIC %md
# MAGIC This Transition probability matrix will act as the base for our subsequent calculations.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3: Calculate the Total Conversion Probability
# MAGIC In this step, we will:
# MAGIC 1. Define `get_transition_probability_graph` utility function
# MAGIC 2. Define `calculate_conversion_probability` utility function
# MAGIC 3. Calculate the Total Conversion Probability

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.1: Define `get_transition_probability_graph` utility function
# MAGIC * This function returns the next stages and associated transition probabilities for every start state.
# MAGIC * We will use this function in Step 3 to calculate the total conversion probability and in Step 4 to calculate the removal effect for each channel.

# COMMAND ----------

def get_transition_probability_graph(removal_state = "null"):
  '''
  This function calculates a subset of the transition probability graph based on the state to exclude
      removal_state: channel that we want to exclude from our Transition Probability Matrix
  returns subset of the Transition Probability matrix as pandas Dataframe
  '''
  
  transition_probability_pandas_df = None
  
  # Get the transition probability graph without any states excluded if the removal_state is null
  if removal_state == "null":
    transition_probability_pandas_df = spark.sql('''select
        trim(start_state) as start_state,
        collect_list(end_state) as next_stages,
        collect_list(transition_probability) as next_stage_transition_probabilities
      from
        transition_matrix
      group by
        start_state''').toPandas()
    
  # Otherwise, get the transition probability graph with the specified channel excluded/removed
  else:
    transition_probability_pandas_df = spark.sql('''select
      sub1.start_state as start_state,
      collect_list(sub1.end_state) as next_stages,
      collect_list(transition_probability) as next_stage_transition_probabilities
      from
      (
        select
          trim(start_state) as start_state,
          case
            when end_state == \"'''+removal_state+'''\" then 'Null'
            else end_state
          end as end_state,
          transition_probability
        from
          transition_matrix
        where
          start_state != \"'''+removal_state+'''\"
      ) sub1 group by sub1.start_state''').toPandas()

  return transition_probability_pandas_df

# COMMAND ----------

transition_probability_pandas_df = get_transition_probability_graph()

# COMMAND ----------

transition_probability_pandas_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.2: Define `calculate_conversion_probability` utility function
# MAGIC * This utility function returns the total conversion probability based on the provided Transition Probability Matrix in pandas data frame format

# COMMAND ----------

def calculate_conversion_probability(transition_probability_pandas_df, calculated_state_conversion_probabilities, visited_states, current_state="Start"):
  '''
  This function calculates total conversion probability based on a subset of the transition probability graph
    transition_probability_pandas_df: This is a Dataframe that maps the current state to all probable next stages along with their transition probability
    removal_state: the channel that we want to exclude from our Transition Probability Matrix
    visited_states: set that keeps track of the states that have been visited thus far in our state transition graph.
    current_state: by default the start state for the state transition graph is Start state
  returns conversion probability of current state/channel 
  '''
 
  #If the customer journey ends with conversion return 1
  if current_state=="Conversion":
    return 1.0
  
  #If the customer journey ends without conversion, or if we land on the same state again, return 0.
  #Note: this step will mitigate looping on a state in the event that a customer path contains a transition from a channel to that same channel.
  elif (current_state=="Null") or (current_state in visited_states):
    return 0.0
  
  #Get the conversion probability of the state if its already calculated
  elif current_state in calculated_state_conversion_probabilities.keys():
    return calculated_state_conversion_probabilities[current_state]
  
  else:
  #Calculate the conversion probability of the new current state
    #Add current_state to visited_states
    visited_states.add(current_state)
    
    #Get all of the transition probabilities from the current state to all of the possible next states
    current_state_transition_df = transition_probability_pandas_df.loc[transition_probability_pandas_df.start_state==current_state]
    
    #Get the next states and the corresponding transition probabilities as a list.
    next_states = current_state_transition_df.next_stages.to_list()[0]
    next_states_transition_probab = current_state_transition_df.next_stage_transition_probabilities.to_list()[0]
    
    #This will hold the total conversion probability of each of the states that are candidates to be visited next from the current state.
    current_state_conversion_probability_arr = []
    
    #Call this function recursively until all states in next_states have been incorporated into the total conversion probability
    import copy
    #Loop over the list of next states and their transition probabilities recursively
    for next_state, next_state_tx_probability in zip(next_states, next_states_transition_probab):
      current_state_conversion_probability_arr.append(next_state_tx_probability * calculate_conversion_probability(transition_probability_pandas_df, calculated_state_conversion_probabilities, copy.deepcopy(visited_states), next_state))
    
    #Sum the total conversion probabilities we calculated above to get the conversion probability of the current state.
    #Add the conversion probability of the current state to our calculated_state_conversion_probabilities dictionary.
    calculated_state_conversion_probabilities[current_state] =  sum(current_state_conversion_probability_arr)
    
    #Return the calculated conversion probability of the current state.
    return calculated_state_conversion_probabilities[current_state]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3.3: Calculate the total conversion probability 

# COMMAND ----------

total_conversion_probability = calculate_conversion_probability(transition_probability_pandas_df, {}, visited_states=set(), current_state="Start")

# COMMAND ----------

total_conversion_probability

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Use Removal Effect to Calculate Attribution
# MAGIC 
# MAGIC In this step, we will:
# MAGIC 1. Calculate the removal effect per channel
# MAGIC 2. Calculate conversion attribution per channel
# MAGIC 3. Register conversion_pandas_df as table to use SQL
# MAGIC 4. View channel attribution
# MAGIC 5. Merge channel attribution results into gold_attribution table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.1: Calculate the removal effect per channel

# COMMAND ----------

removal_effect_per_channel = {}
for channel in transition_probability_pandas_df.start_state.to_list():
  if channel!="Start":
    transition_probability_subset_pandas_df = get_transition_probability_graph(removal_state=channel)
    new_conversion_probability =  calculate_conversion_probability(transition_probability_subset_pandas_df, {}, visited_states=set(), current_state="Start")
    removal_effect_per_channel[channel] = round(((total_conversion_probability-new_conversion_probability)/total_conversion_probability), 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.2: Calculate conversion attribution per channel

# COMMAND ----------

conversion_attribution={}

for channel in removal_effect_per_channel.keys():
  conversion_attribution[channel] = round(removal_effect_per_channel[channel] / sum(removal_effect_per_channel.values()), 2)

channels = list(conversion_attribution.keys())
conversions = list(conversion_attribution.values())

conversion_pandas_df= pd.DataFrame({'attribution_model': 
                                    ['markov_chain' for _ in range(len(channels))], 
                                    'channel':channels, 
                                    'attribution_percent': conversions})


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.3: Register `conversion_pandas_df` as table to use SQL

# COMMAND ----------

sparkDF=spark.createDataFrame(conversion_pandas_df) 
sparkDF.createOrReplaceTempView("markov_chain_attribution_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.4: View channel attribution

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from markov_chain_attribution_update

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4.5: Merge channel attribution results into `gold_attribution` table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_attribution
# MAGIC USING markov_chain_attribution_update
# MAGIC ON markov_chain_attribution_update.attribution_model = gold_attribution.attribution_model AND markov_chain_attribution_update.channel = gold_attribution.channel
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Compare Channel Performance Across Methods

# COMMAND ----------

attribution_pd = spark.table('gold_attribution').toPandas()

sns.set(font_scale=1.1)
sns.catplot(x='channel',y='attribution_percent',hue='attribution_model',data=attribution_pd, kind='bar', aspect=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Build a dashboard that can be used for optimizing marketing budget allocation

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
