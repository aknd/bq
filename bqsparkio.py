# coding:utf-8

import os
import subprocess
import json
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class BQSparkIO(object):
    def __init__(self, spark, bucket_name):
        self.spark = spark
        self.bucket_name = bucket_name

    def rm_gcs_tmp_dir(self, folder_name):
        tdir = 'gs://{}/hadoop/tmp/bigquery/{}'.format(
            self.bucket_name, folder_name)
        tpath = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(tdir)
        tpath.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())\
            .delete(tpath, True)

    def get_rdd_from_bq_via_gcs(self, project_id, dataset_name, table_fullname, gs_overwrite=False):
        bucket = self.spark.sparkContext._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
        project = self.spark.sparkContext._jsc.hadoopConfiguration().get('fs.gs.project.id')
        folder_name = '{}.{}.{}'.format(dataset_name, table_fullname, uuid.uuid4())
        input_directory = 'gs://{}/hadoop/tmp/bigquery/{}'.format(
            self.bucket_name, folder_name)

        conf = {
            'mapred.bq.project.id': project,
            'mapred.bq.gcs.bucket': bucket,
            'mapred.bq.temp.gcs.path': input_directory,
            'mapred.bq.input.project.id': project_id,
            'mapred.bq.input.dataset.id': dataset_name,
            'mapred.bq.input.table.id': table_fullname,
            'mapred.bq.input.sharded.export.enable': False,
        }

        if gs_overwrite:
            self.rm_gcs_tmp_dir(folder_name)

        table_data = self.spark.sparkContext.newAPIHadoopRDD(
            'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'com.google.gson.JsonObject',
            conf=conf)

        print('Data imported from table {}.{}'.format(dataset_name, table_fullname))

        return table_data

    def export_rdd_via_gcs_to_bq(self, output_rdd, schema_filename, dataset_name, table_fullname, append=False, gs_overwrite=False):
        local_schema_filepath = '/tmp/{}'.format(schema_filename)

        subprocess.check_call(
            'gsutil cp gs://{}/src/main/resources/schema/{} /tmp/{}'.format(
                self.bucket_name, schema_filename, schema_filename).split())

        fields = list(map(
            lambda s: s['name'], json.load(open(local_schema_filepath))))

        folder_name = '{}.{}.{}'.format(dataset_name, table_fullname, uuid.uuid4())
        output_directory = 'gs://{}/hadoop/tmp/bigquery/{}'\
            .format(self.bucket_name, folder_name)
        partitions = range(output_rdd.getNumPartitions())
        output_files = [output_directory + '/part-{:05}'.format(i) for i in partitions]

        if gs_overwrite:
            self.rm_gcs_tmp_dir(folder_name)

        (output_rdd
         .map(lambda line: json.dumps(dict(zip(fields, line))))
         .saveAsTextFile(output_directory))

        if append:
            subprocess.check_call(
                'bq load --source_format NEWLINE_DELIMITED_JSON '
                '{dataset}.{table} {files}'.format(
                    dataset=dataset_name,
                    table=table_fullname,
                    files=','.join(output_files)
                ).split())
        else:
            subprocess.check_call(
                'bq load --source_format NEWLINE_DELIMITED_JSON '
                '--schema={schema_option} '
                '{dataset}.{table} {files}'.format(
                    schema_option=local_schema_filepath,
                    dataset=dataset_name,
                    table=table_fullname,
                    files=','.join(output_files)
                ).split())

        print('Data exported into table {}.{}'.format(dataset_name, table_fullname))

    def union_all(self, dfs):
        if len(dfs) > 1:
            return dfs[0].union(self.union_all(dfs[1:]))
        else:
            return dfs[0]

    def export_dfs(self, df_list, schema_filename, dataset_name, table_fullname, per=10, partition=1, append=False, gs_overwrite=False):
        if not isinstance(df_list, list):
            df_list = [df_list]

        index = 0

        while len(df_list) > 0:
            export_list = []
            for _i in range(per):
                if len(df_list) > 0:
                    export_list.append(df_list.pop(0))
                else:
                    break
            self.export_rdd_via_gcs_to_bq(
                self.union_all(export_list).rdd.repartition(partition),
                schema_filename,
                dataset_name,
                table_fullname,
                append=append,
                gs_overwrite=gs_overwrite)
            if index == 0:
                append = True
                gs_overwrite = False
            index += 1
