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
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from keras.layers import Dense
from keras.models import Sequential


def train_save_model(modelname, traindata):
    field_list = ['ntp', 'pgc', 'dbp', 'tsft', 'si', 'bmi', 'dpf', 'age', 'class']

    for line in traindata:
        print(line)

    csv_dict = csv.DictReader(traindata, field_list)

    # Create the DataFrame
    df = pd.DataFrame(csv_dict)
    df = df.apply(pd.to_numeric)
    dataset = df.values
    print(dataset)
    # split into input (X) and output (Y) variables
    X = dataset[:, 0:8]
    Y = dataset[:, 8]
    # define model
    model = Sequential()
    model.add(Dense(12, input_dim=8, activation='relu'))
    model.add(Dense(8, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))
    # compile model
    model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    # Fit the model
    model.fit(X, Y, epochs=150, batch_size=10, verbose=0)
    # evaluate the model
    scores = model.evaluate(X, Y, verbose=0)

    model.save(modelname)
    # Save to GCS
    logging.info("Saved the model %s ", modelname)

    return ['%0.3f, %0.3f' % (scores[1], scores[0])]


def split_dataset(plant, num_partitions, ratio):
    assert num_partitions == len(ratio)
    bucket = sum(map(ord, json.dumps(plant))) % sum(ratio)
    total = 0
    for i, part in enumerate(ratio):
        total += part
        if bucket < total:
            return i
    return len(ratio) - 1


def order_dict_to_csv(odict):
    return ''.join(odict.values())


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

        test_dataset | 'WriteTest' >> WriteToText(file_path_prefix="results/test", file_name_suffix=".csv")
        # See https://beam.apache.org/documentation/patterns/side-inputs/
        # https://beam.apache.org/releases/pydoc/2.3.0/apache_beam.pvalue.html#apache_beam.pvalue.AsIter
        output = (p | 'GetModelName' >> beam.Create(['model.h5'])
                  | 'TrainAndSaveModel' >> beam.FlatMap(
                    train_save_model, traindata=beam.pvalue.AsList(train_dataset)))

        output | 'WriteModelPerformance' >> WriteToText(file_path_prefix="results/modelperformance",
                                                        file_name_suffix=".csv")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
