from dbacademy.dbgems import get_cloud
def get_job_param_json(env, solacc_path, job_name, node_type_id, spark_version, spark):
    
    # This job is not environment specific, so `env` is not used
    num_workers = 2
    job_json = {
        "timeout_seconds": 7200,
        "name": job_name,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "CME"
        },
        "tasks": [
            {
                "job_cluster_key": "multitouch_cluster",
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/01_intro"
                },
                "task_key": "multitouch_01"
            },
            {
                "job_cluster_key": "multitouch_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/02_load_data"
                },
                "task_key": "multitouch_02",
                "depends_on": [
                    {
                        "task_key": "multitouch_01"
                    }
                ]
            },
            {
                "job_cluster_key": "multitouch_cluster",
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/03_prep_data"
                },
                "task_key": "multitouch_03",
                "depends_on": [
                    {
                        "task_key": "multitouch_02"
                    }
                ]
            },
            {
                "job_cluster_key": "multitouch_cluster",
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/04_markov_chains"
                },
                "task_key": "multitouch_04",
                "depends_on": [
                    {
                        "task_key": "multitouch_03"
                    }
                ]
            },
            {
                "job_cluster_key": "multitouch_cluster",
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/05_spend_optimization",
                    "base_parameters": {
                      "adspend": "10000"
                    }
                },
                "task_key": "multitouch_05",
                "depends_on": [
                    {
                        "task_key": "multitouch_04"
                    }
                ]
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "multitouch_cluster",
                "new_cluster": {
                    "spark_version": spark_version,
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "num_workers": num_workers,
                    "node_type_id": node_type_id,
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }
    cloud = get_cloud()
    if cloud == "AWS": 
      job_json["job_clusters"][0]["new_cluster"]["aws_attributes"] = {
                        "ebs_volume_count": 0,
                        "availability": "ON_DEMAND",
                        "instance_profile_arn": "arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access",
                        "first_on_demand": 1
                    }
    if cloud == "MSA": 
      job_json["job_clusters"][0]["new_cluster"]["azure_attributes"] = {
                        "availability": "ON_DEMAND_AZURE",
                        "first_on_demand": 1
                    }
    if cloud == "GCP": 
      job_json["job_clusters"][0]["new_cluster"]["gcp_attributes"] = {
                        "use_preemptible_executors": False
                    }
    return job_json

    