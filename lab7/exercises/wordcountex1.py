#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from past.builtins import unicode

def printAndReturn(element):
    print(element)
    return element

def startWithAorB(word_count_pair):
    word, count = word_count_pair
    word_lower = word.lower()
    if word_lower.startswith('a'):
        return [('StartwithA_key', count)]
    elif word_lower.startswith('b'):
        return [('StartwithB_key', count)]
    else:
        return []


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--output2',
        dest='output2',
        required=True,
        help='Output file to write second results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> ReadFromText(known_args.input)

        counts = (
                lines
                | 'Split' >>
                (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
                | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the counts into a PCollection of strings.
        def format_result(word, count):
            return '%s: %d' % (word, count)

        output = counts | 'Format' >> beam.MapTuple(format_result)

        output2 = (counts
                   | 'StartWithAorB' >> beam.FlatMap(startWithAorB)
                   | 'StartWithAorBPrint' >> beam.Map(printAndReturn)
                   | 'GroupAndSumStartWithAorB' >> beam.CombinePerKey(sum)
                   | 'FormatSumStartWithAorB' >> beam.MapTuple(format_result))

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        output | 'WriteAll' >> WriteToText(known_args.output)
        output2 | 'WriteStartWithAorB' >> WriteToText(known_args.output2)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
