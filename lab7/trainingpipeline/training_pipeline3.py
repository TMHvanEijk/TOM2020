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
import csv
import io
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from keras.models import load_model


def get_csv_reader(readable_file):
    field_list = ['ntp', 'pgc', 'dbp', 'tsft', 'si', 'bmi', 'dpf', 'age', 'class']
    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)

    # Return the csv reader
    return csv.DictReader(io.TextIOWrapper(gcs_file), field_list)


class MyPredictDoFn(beam.DoFn):

    def __init__(self):
        self._model = None

    def setup(self):
        logging.info("MyPredictDoFn initialisation. Load Model")
        self._model = load_model('model.h5')

    def process(self, elements, **kwargs):
        for line in elements:
            print(line)
        df = pd.DataFrame(elements)
        print(df)
        df = df.drop('class', 1)
        # df = pd.DataFrame.from_dict(elements,
        #                             orient="index").transpose().fillna(0)
        df = df.apply(pd.to_numeric)
        val_set2 = df.copy()
        logging.info(val_set2)
        result = self._model.predict(df)
        y_classes = result.argmax(axis=-1)
        val_set2['class'] = y_classes.tolist()
        dic = val_set2.to_dict(orient='records')
        return [dic]


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
        # Read the text file[pattern] into a PCollection.
        prediction_data = (p | 'CreatePCollection' >> beam.Create(['results/test-00000-of-00001.csv'])
                           | 'ReadCSVFle' >> beam.FlatMap(get_csv_reader))

        # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
        # https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/
        output = (prediction_data
                  | 'batch into n batches' >> beam.BatchElements(min_batch_size=50, max_batch_size=75)
                  | 'Predict' >> beam.ParDo(MyPredictDoFn()))

        output | 'WritePredictionResults' >> WriteToText(file_path_prefix="results/predictions",
                                                         file_name_suffix=".json")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
