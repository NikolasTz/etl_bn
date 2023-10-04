
<!-- TOC -->
## Table of Contents
  * [Description](#description)
  * [Architecture](#architecture)
  * [Configuration](#configuration)
  * [Requirements](#requirements)
<!-- TOC -->

## Description

A simple ETL process for Bayesian Network using Apache Airflow,Apache Kafka and HDFS.

The integration of the Apache Airflow boils down to the fact that we want to exploit
the functionality of provided scheduler in order to offer the following steps scheduled:

    1. Load the dataset from the inputPath local directory
    2. Make some transforms(cleansing,add partition_id column) on dataset and write some stats of it to the HDFS directory
    3. Load the dataset from the HDFS directory and write it to Apache Kafka

## Architecture
![Arch](img/etl_bn)

## Configuration

The configuration of dag run can be passed either using the argument **--config** or through **UI**

    Hadoop => host,port
    Kafka => topic,servers,numPartitions
    Local => inputPath

```json
"inputPath": "path_to_local_dir",
"host": "host_url",
"port": "port_number",
"topic": "topic_name",
"servers": "bootstrap_servers",
"numPartitions": "partitions"
```

## Requirements
* [Requirements](requirements/requirements.txt)
