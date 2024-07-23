# Import required modules and methods
import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions
import json
import ast

# Setting up the Apache Beam pipeline options.
beam_options = PipelineOptions(
    save_main_session=True,
    # runner='DirectRunner',
    runner='DataflowRunner',
    project='gwc-poc',
    temp_location='gs://composer14/temp',
    region='us-central1')


# ParDo Class for parallel processing by applying user defined tranformations
class ParseJSON(beam.DoFn):
    def process(self, element):
        try:
            dict_line = json.loads(element)
            sub_str = dict_line['protoPayload']['methodName']
            if 'google.cloud' in sub_str:
                sub_str = sub_str.split('.')[4] + '.' + sub_str.split('.')[5]
            st = '{' + "'user':'" + dict_line['protoPayload']['authenticationInfo'][
                'principalEmail'] + "','job_type':'" + sub_str.lower().rstrip('job') + "','info_type':'" + dict_line[
                     'severity'] + "','timestamp':'" + dict_line['timestamp'] + "'}"
            st = st.replace("'", '"')
            return st.split('\n')
        except:
            logging.info('Some Error occured')


# Entry Function to run Pipeline
def run():
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    with beam.Pipeline(options=beam_options) as p:
        result = (
                p | 'Read from GCS' >> ReadFromText(
            'gs://logs_exports14/cloudaudit.googleapis.com/data_access/2024/07/23/*.json')
                | 'Parse logs to string representation of dict' >> beam.ParDo(ParseJSON())
                | 'Convert String to Dict' >> beam.Map(lambda x: json.loads(x))
            # | beam.Map(print)
        )

        write_to_gcs = (result | 'get job type tuple' >> beam.Map(lambda x: (x['job_type'] + ',' + x['info_type'], 1))
                        | 'combine per key and sum' >> beam.CombinePerKey(sum)
                        | 'format to JSON' >> beam.Map(lambda x: "{'job_type':'" + x[0].split(',')[0] +
                                                                 "','info_type':'" + x[0].split(',')[
                                                                     1] + "','count':" + str(x[1]) + "}")
                        # | beam.Map(print)
                        | 'write final results into GCS bucket' >> beam.io.WriteToText('gs://data_transformed14/')
                        )

        write_to_bq = result | 'Write parsed results to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
            'bq_auditlog_parsed_data',
            dataset='logs_export',
            project='gwc-poc',
            schema='SCHEMA_AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()