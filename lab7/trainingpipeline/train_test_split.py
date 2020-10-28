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

from __future__ import absolute_import

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# see https://beam.apache.org/documentation/transforms/python/elementwise/partition/
def split_dataset(dataset, num_partitions, ratio):
    assert num_partitions == len(ratio)
    bucket = sum(map(ord, json.dumps(dataset))) % sum(ratio)
    total = 0
    for i, part in enumerate(ratio):
        total += part
        if bucket < total:
            return i
    return len(ratio) - 1


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        train_dataset, test_dataset = (p | 'Create FileName Object' >> ReadFromText('data/pima-indians-diabetes.csv')
                                       | 'Train_Test_Split' >> beam.Partition(split_dataset, 2, ratio=[8, 2]))
        train_dataset | 'WriteTrain' >> WriteToText(file_path_prefix="split/train", file_name_suffix=".csv")
        test_dataset | 'WriteTest' >> WriteToText(file_path_prefix="split/test", file_name_suffix=".csv")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
