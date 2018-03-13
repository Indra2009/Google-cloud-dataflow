from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
  """Main entry point; defines and runs the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
				default=<bucket/file>,
				help='Input file to process.')
				
  parser.add_argument(
				'--output',
				default=<project-name>:<dataset>.<table>',
				help=
				('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
				'or DATASET.TABLE.'))
   				
  known_args, pipeline_args = parser.parse_known_args(argv)
  
  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=<project-name>',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=<bucket-loaction>',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=<bucket-loaction>',
      '--job_name=bucket_to_table',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    #lines = p | ReadFromText(known_args.input)
	
	def to_table_row(x):
		values = x.split(',')
		return { 'field1': values[0], 'field2': values[1] } 

	lines = p | 'read' >> ReadFromText(known_args.input)
	
	lines | 'ToTableRows' >> beam.Map(to_table_row) | 'write2' >> beam.io.Write(beam.io.BigQuerySink(
											known_args.output,
											schema='field1:INTEGER, field2:STRING'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()