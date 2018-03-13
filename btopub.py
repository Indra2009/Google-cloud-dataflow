from __future__ import absolute_import

import argparse
import logging
import re
import unittest


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.pubsub import WriteStringsToPubSub
from apache_beam.options.pipeline_options import StandardOptions

	

def run(argv=None):
  """Run the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                default=<file-location-in-bucket>,
                help=('File from Bucket'))

  parser.add_argument(
      '--output',
      default=<pubsub-topic>,
      help=
      ('Output Pubsub topic '))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
  
	lines = p | 'read' >> ReadFromText(known_args.input) | beam.Map(lambda x: x) |  'Write' >> beam.io.WriteStringsToPubSub(known_args.output)
		
    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()