from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam


def run(argv=None):
  """Run the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                default='<project-name>:<dataset>.Tornado',
                help=('Input BigQuery table to process specified as: '
                'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

  parser.add_argument(
      '--output',
      default='<project-name>:<dataset>.Tornadov2',
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
  
	rows = p | 'read' >> beam.io.Read(beam.io.BigQuerySource(known_args.input))

	rows | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema='month:INTEGER, tornado_count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()