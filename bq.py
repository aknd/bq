# coding:utf-8

import sys
import re
import time
import uuid

from datetime import datetime
from pytz import timezone
from dateutil.relativedelta import relativedelta

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import google.cloud.bigquery.job

import httplib2
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

from bigquery import get_client
from bigquery.client import BigQueryClient

# DATE_ORIGIN_DEFAULT = '20150201'
DATE_ORIGIN_DEFAULT = '20160401'
DATE_STR_LEN = 8


class BQClient(object):
    def __init__(self,
            project_id,
            bigquery=bigquery,
            date_origin_default=DATE_ORIGIN_DEFAULT,
            date_str_len=DATE_STR_LEN,
            swallow_results=True):
        self.project_id = project_id
        self.date_origin_default = date_origin_default
        self.date_str_len = date_str_len
        self.gcb_client = bigquery.Client(project=project_id)

        credentials = GoogleCredentials.get_application_default()
        http = credentials.authorize(httplib2.Http())
        service = build('bigquery', 'v2', http=http)

        self.bp_client = BigQueryClient(service, project_id, swallow_results)

    def wait_for_job(self, job):
        while True:
            job.reload()
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return
            time.sleep(1)

    def fetch_data(self, query):
        print('Execute query:\n{}'.format(query))

        query_job = self.gcb_client.run_async_query(str(uuid.uuid4()), query)
        query_job.use_legacy_sql = True
        query_job.begin()

        self.wait_for_job(query_job)

        query_results = query_job.result()
        page_token = None

        rows = []
        append = rows.append

        # while True:
        #     page_rows, total_rows, page_token = query_results.fetch_data(
        #         max_results=10 ** 8,
        #         page_token=page_token)

        #     for row in page_rows:
        #         append(row)

        #     if not page_token:
        #         break

        itr = query_results.fetch_data(
            max_results=10 ** 8,
            page_token=page_token)
        for page in itr.pages:
            for row in list(page):
                append(row)

        fields = [sch.name for sch in query_results.schema]

        return rows, fields

    def does_table_exists(self, dataset_name, table_fullname):
        dataset = self.gcb_client.dataset(dataset_name)

        if not dataset.exists():
            print('Dataset {} does not exist'.format(dataset_name))
            return

        table = dataset.table(table_fullname)

        return table.exists()

    def create_table(self, dataset_name, table_fullname, schema=None, view_query=None, overwrite=False):
        if schema is None and view_query is None:
            sys.exit('Either schema or view_query is required')

        dataset = self.gcb_client.dataset(dataset_name)

        if not dataset.exists():
            print('Dataset {} does not exist'.format(dataset_name))
            return

        table = dataset.table(table_fullname)

        if table.exists():
            if overwrite:
                table.delete()
                print('Existing table {}.{} deleted'\
                    .format(dataset_name, table_fullname))
            else:
                sys.exit('Table {}.{} already exists'\
                    .format(dataset_name, table_fullname))

        if schema is not None:
            table.schema = [
                SchemaField(
                    field['name'],
                    field['type'],
                    field['mode'],
                    field['description'])
                        for field in schema
            ]

        if view_query is not None:
            print('View query:\n{}'.format(view_query))
            table.view_query = view_query

        table.create()

        print('Table {} created in dataset {}'.format(table_fullname, dataset_name))

    def delete_table(self, dataset_name, table_fullname):
        dataset = self.gcb_client.dataset(dataset_name)

        if not dataset.exists():
            print('Dataset {} does not exist'.format(dataset_name))
            return

        table = dataset.table(table_fullname)

        if table.exists():
            table.delete()
            print('Existing table {}.{} deleted'\
                .format(dataset_name, table_fullname))

    def insert_data(self, dataset_name, table_fullname, rows):
        dataset = self.gcb_client.dataset(dataset_name)

        if not dataset.exists():
            print('Dataset {} does not exist'.format(dataset_name))
            return

        table = dataset.table(table_fullname)

        if not table.exists():
            print('Table {}.{} does not exist'\
                .format(dataset_name, table_fullname))
            return

        if len(table.schema) == 0:
            table.reload()

        table.insert_data(rows)

        print('Data inserted into table {}.{}'\
            .format(dataset_name, table_fullname))

    def write_to_table(self,
            dataset_name,
            table_fullname,
            query,
            allow_large_results=False,
            maximum_billing_tier=1):
        print('Execute query:\n{}'.format(query))

        client = self.bp_client

        job = client.write_to_table(
            query,
            dataset_name,
            table_fullname,
            allow_large_results=allow_large_results,
            maximum_billing_tier=maximum_billing_tier)
        job_resource = client.wait_for_job(job, timeout=720)

        print('Query result inserted into table {}.{}'\
            .format(dataset_name, table_fullname))

        return job_resource

    def get_table_dates(self, dataset_name, table_name):
        dataset = self.gcb_client.dataset(dataset_name)

        if not dataset.exists():
            print('Dataset {} does not exist'.format(dataset_name))
            return

        dates = []
        append = dates.append

        for table in dataset.list_tables():
            pattern = r'^%s[\d]{%s,}$' % (table_name, DATE_STR_LEN)

            if re.search(pattern, table.name):
                date = table.name[-self.date_str_len:]
                append(date)

        return dates

    def get_target_dates(self,
            origin_dataset_name,
            origin_table_name,
            destination_dataset_name,
            destination_table_name,
            date_origin=DATE_ORIGIN_DEFAULT,
            include_existing=False):
        origin_table_dates = self.get_table_dates(
            origin_dataset_name,
            origin_table_name)

        if len(origin_table_dates) == 0:
            return []

        jst_now = datetime.now(timezone('Asia/Tokyo'))
        two_years_ago = jst_now + relativedelta(years=-2)
        date_origin = max(
            date_origin or DATE_ORIGIN_DEFAULT,
            two_years_ago.strftime('%Y%m%d'),
            self.date_origin_default)

        start = max(origin_table_dates[0], date_origin)
        end = origin_table_dates[-1]

        daterange = self._daterange(start, end)

        dates = []

        if include_existing:
            dates = [date
                for date in daterange
                    if date in origin_table_dates]
        else:
            destination_table_dates = self.get_table_dates(
                destination_dataset_name,
                destination_table_name)

            dates = [date
                for date in daterange
                    if date in origin_table_dates
                            and
                        date not in destination_table_dates]

        return dates

    def _daterange(self, start, end):
        start_date = datetime.strptime(start, '%Y%m%d')
        end_date = datetime.strptime(end, '%Y%m%d')

        for n in range((end_date - start_date).days + 1):
            yield (start_date + relativedelta(days=+n)).strftime('%Y%m%d')
