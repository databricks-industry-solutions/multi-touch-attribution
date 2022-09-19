<div >
  <img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/solution-accelerator-logo.png"; width="90%">
</div>

## Overview

Behind the growth of every consumer-facing product is the acquisition and retention of an engaged user base. When it comes to acquisition, the goal is to attract high quality users as cost effectively as possible. With marketing dollars dispersed across a wide array of campaigns, channels, and creatives, however, measuring effectiveness is a challenge. In other words, it's difficult to know how to assign credit where credit is due. Enter multi-touch attribution. With multi-touch attribution, credit can be assigned in a variety of ways, but at a high-level, it's typically done using one of two methods: `heuristic` or `data-driven`.

* Broadly speaking, heuristic methods are rule-based and consist of both `single-touch` and `multi-touch` approaches. Single-touch methods, such as `first-touch` and `last-touch`, assign credit to the first channel, or the last channel, associated with a conversion. Multi-touch methods, such as `linear` and `time-decay`, assign credit to multiple channels associated with a conversion. In the case of linear, credit is assigned uniformly across all channels, whereas for time-decay, an increasing amount of credit is assigned to the channels that appear closer in time to the conversion event.

* In contrast to heuristic methods, data-driven methods determine assignment using probabilites and statistics. Examples of data-driven methods include `Markov Chains` and `SHAP`. In this series of notebooks, we cover the use of Markov Chains and include a comparison to a few heuristic methods.

## About This Series of Notebooks

* This series of notebooks is intended to help you use multi-touch attribution to optimize your marketing spend.

* In support of this goal, we will:
 * Generate synthetic ad impression and conversion data.
 * Create a streaming pipeline for processing ad impression and conversion data in near real-time.
 * Create a batch pipeline for managing summary tables used for reporting, ad hoc queries, and decision support.
 * Calculate channel attribution using Markov Chains.
 * Create a dashboard for monitoring campaign performance and optimizing marketing spend.

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

|Library Name|Library license | Library License URL | Library Source URL |
|---|---|---|---|
|Matplotlib|Python Software Foundation (PSF) License |https://matplotlib.org/stable/users/license.html|https://github.com/matplotlib/matplotlib|
|Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
|Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
|Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
|Seaborn|BSD-3-Clause License|https://github.com/mwaskom/seaborn/blob/master/LICENSE|https://github.com/mwaskom/seaborn|
|Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
