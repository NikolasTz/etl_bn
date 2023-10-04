# pip install apache-airflow
# pip install apache-airflow-providers-apache-kafka
# pip install hdfs
# pip install numpy
# pip install pandas


"""
    A simple ETL process for Bayesian Network using Apache Airflow,Apache Kafka and HDFS
    Load the dataset from the inputPath directory
    Make some transforms(cleansing,add partition_id column) on dataset and write some stats to the HDFS
    Load the dataset from the HDFS and write the dateset to Apache Kafka

    The configuration of dag run can be passed either using the argument --config or through UI

    Parameters
    Hadoop => host, port
    Kafka => topic,servers,numPartitions
    Local => inputPath

"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import DagRun, TaskInstance

default_args = {
    "owner": "NTZ",
    "depend_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to topic={} , part={} and key={}'.format(msg.topic(), msg.partition(), msg.key()))


@dag(dag_id="etl_bn",
     catchup=False,
     schedule="@daily",
     start_date=datetime(2023, 10, 4),
     default_args=default_args,
     tags=["etl_bn", "bn"],
     description="A simple ETL for BN datasets using Apache Airflow,Kafka and HDFS")
def bn_etl():
    # Setup connections for HADOOP and KAFKA using Externalized Parameters
    @task(task_id="load_conn")
    def load_conn(**kwargs):

        """
            :param kwargs: context
            :return: None

            Setup connections for HADOOP and KAFKA using Externalized Parameters
        """

        from airflow.models import Connection
        from airflow.utils import db
        import json

        # Hadoop connection
        db.merge_conn(
            Connection(
                conn_id="hd_conn",
                conn_type="hadoop",
                host="{{ dag_run.conf['host'] }}",
                port=int("{{ dag_run.conf['port'] }}"),

            )
        )

        # Kafka connection
        db.merge_conn(
            Connection(
                conn_id="kafka_conn",
                conn_type="kafka",
                extra=json.dumps(
                    {
                        "bootstrap.servers": "{{ dag_run.conf['servers'] }}",
                        "group.id": "{{ dag_run.conf['topic'] }}" + "-group",
                        "acks": "all",
                        "retries": "0",
                        "linger.ms": "0",
                        "key.serializer": "confluent_kafka.serialization.IntegerSerializer",
                        "value.serializer": "confluent_kafka.serialization.StringSerializer"
                    }
                )
            )
        )

    # Load the dataset from inputPath directory, cleanse the dataset, add partition id column and write to an HDFS
    @task(task_id="transform")
    def transform(**kwargs):
        """
            :param kwargs: context
            :return: None

            Load the dataset from @inputPath directory.\n
            Cleansing of dataset.\n
            Add partition_id column.\n
            Write to an HDFS, specified on @host and @port parameters
        """

        from airflow.exceptions import AirflowConfigException

        # Read the dataset from inputPath

        # Get dag run configuration
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run

        # Check the inputPath config
        if "inputPath" not in dag_run.conf:
            raise AirflowConfigException("No inputPath parameter provided")

        # Check the numPartition parameter
        if "numPartitions" not in dag_run.conf:
            raise AirflowConfigException("No numPartitions parameter provided")

        import os
        if not os.path.exists(dag_run.conf["inputPath"]):
            raise AirflowConfigException("No such a file or directory")

        if int(dag_run.conf["numPartitions"]) < 0:
            raise AirflowConfigException("Negative number of partitions")

        # Read the file from the inputPath directory
        import re
        import pandas as pd
        import numpy as np

        count = 0
        find_schema = False
        df = pd.DataFrame()

        # Data validation
        with open(dag_run.conf["inputPath"], mode="r") as f:
            while True:

                # Read line
                line = f.readline()
                count = count + 1

                # EOF
                if not line:
                    break

                # Skip empty lines
                if line == "":
                    continue

                # Strips off all non-ASCII characters
                line = re.sub(pattern="[^\\x00-\\x7F]", repl="", string=line)

                # Erases all the ASCII control characters
                line = re.sub(pattern=r"[\x01-\x1F\x7F]", repl="", string=line)

                # Get the schema and update dataframe
                if not find_schema:
                    df = pd.DataFrame(columns=(line + ",PARTITION").split(","))
                    # print("Find schema")
                    # print(df)
                    find_schema = True
                    continue

                # Add values to dataframe
                df.loc[len(df)] = (line + "," + str(np.random.randint(0, 8, dtype=int))).split(sep=",")

        # Write the file to csv file
        # df.to_csv(os.path.dirname(dag_run.conf["inputPath"])+"\\tmp.csv", index=False, header=False)

        # Write the file to HDFS

        # Create an HDFS client
        from airflow.hooks.base import BaseHook
        from hdfs.client import Client

        # Get the parameters from connection
        # Example : hdfs://quickstart.cloudera:8020/user/Cloudera/Test
        conn = BaseHook.get_connection("hd_conn")
        hdfs_path = "hdfs://" + conn.host + ":" + str(conn.port)

        # HDFS client
        hdfs_client = Client(url=hdfs_path)

        # Paths
        data_path = hdfs_path + "//tmp" + kwargs["ts"] + ".csv"
        stats_path = hdfs_path + "//stats_" + kwargs["ts"]

        # Write data
        with hdfs_client.write(hdfs_path=data_path) as writer:
            df.to_csv(writer)

        # Write stats
        num_dash = 30

        # Size of dataset
        size = "rows:" + str(df.shape[0]) + "\n" + "columns:" + str(df.shape[1])
        hdfs_client.write(hdfs_path=stats_path, data=size + "\n")
        hdfs_client.write("-" * num_dash + "\n")

        # Partition distribution
        par = df.value_counts("PARTITION", sort=False).reset_index()
        par.columns = ['PARTITION_ID', 'DIST']  # Change column names
        hdfs_client.write(hdfs_path=stats_path, data=par.to_string(index=False).strip() + "\n")
        hdfs_client.write(hdfs_path=stats_path, data="-" * num_dash + "\n")

        # Column stats
        df_col_stats = df.iloc[:, :len(df.columns) - 1]
        col_stats = df_col_stats.describe(exclude=[np.number]).loc[["unique", "freq", "top"]]
        for col in col_stats.columns:
            col_str = col + "\n" + col_stats[col].to_string() + "\n" * 2
            hdfs_client.write(hdfs_path=stats_path, data=col_str)

    # Load the dataset from an HDFS and write to Kafka topic
    @task(task_id="load")
    def load(**kwargs):

        """
            :param kwargs: context
            :return: None

            Load the dataset from an HDFS. \n
            Write to Kafka topic using the parameters.

        """

        # Create a Kafka consumer
        from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

        kafka_hook = KafkaProducerHook(kafka_config_id="kafka_conn")
        producer = kafka_hook.get_producer()

        # Read the data from HDFS
        from airflow.hooks.base import BaseHook
        from hdfs.client import Client

        # Hadoop's connection and HDFS client
        conn = BaseHook.get_connection("hd_conn")
        hdfs_path = "hdfs://" + conn.host + ":" + str(conn.port)
        hdfs_client = Client(url=hdfs_path)
        path = hdfs_path + "//tmp" + kwargs["ts"] + ".csv"

        # Get the kafka topic
        topic = "{{ dag_run.conf['topic'] }}"

        with hdfs_client.read(hdfs_path=path, delimiter="\n") as reader:
            for line in reader:
                # Get the values
                values = line.split(sep=",")

                # Get the value
                value = ','.join(values[:-1])

                # Get the partition
                part = int(values[-1])

                # Write data to kafka topic

                # Trigger any available delivery report callbacks from previous produce() calls
                producer.poll(0)

                # Asynchronously produce a message. The delivery report callback will
                # be triggered from the call to poll() above, or flush() below, when the
                # message has been successfully delivered or failed permanently.
                producer.produce(topic=topic, key=part, value=value, partition=part, callback=delivery_report)

        # Write EOF to the end of each partition - Act as watermark to trigger some future computations
        # Get the number of partitions
        part = int("{{ dag_run.conf['numPartitions'] }}")
        for i in range(0, part):
            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

            # Write
            producer.produce(topic=topic, key=i, value="EOF", partition=i, callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush()

        # Delete tmp file
        # hdfs_client.delete(hdfs_path=path)

    load_conn >> transform >> load


bn_etl()
