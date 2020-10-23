from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)


class ParseActivityEventFn(beam.DoFn):

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):

        row = json.loads(elem)
        try:
            yield {
                'device': row['Device'],
                'user': row['User'],
                'model': row['Model'],
                'activity': row['gt'],
                # Our current column is unixtime nanoseconds (represented as a long), therefore we’re
                # going to have to do a little manipulation to get it into the proper format
                'eventtime': int(row['Creation_Time']) / 1000000000.0,
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


def is_activity_not_null(plant):
    return plant['activity'] != 'null'


class ExtractAndSum(beam.PTransform):
    def __init__(self, field):
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, pcoll):
        return (
                pcoll
                | beam.Map(lambda elem: (elem[self.field] + '_' + elem['activity'], 1))
                | beam.CombinePerKey(sum))


# [START window_and_trigger]
class CalculateActivityCount(beam.PTransform):

    def __init__(self):
        beam.PTransform.__init__(self)

    def expand(self, pcoll):
        return (
                pcoll
                | 'ActivityFixedWindows' >> beam.WindowInto(
            window.FixedWindows(120), accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
                | 'ExtractAndSum' >> ExtractAndSum('user'))


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the hourly_team_score pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        type=str,
        default='C:\Postdoc\DE2020\DE2020\lab8\exercises\data\*.*',
        help='Path to the data file(s) containing game data.')
    parser.add_argument(
        '--output1', type=str, default='ex1out', help='Path to the output 1 file(s).')

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = save_main_session

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Pre-processing
        events = (
                p
                | 'ReadInputText' >> beam.io.ReadFromText(args.input)
                | 'ParseActivityEventFn' >> beam.ParDo(ParseActivityEventFn())
                | 'FilterNullActivity' >> beam.Filter(is_activity_not_null)
                | 'AddEventTimestamps' >> beam.Map(
            lambda elem: window.TimestampedValue(elem, elem['eventtime'])))

        def format_user_activity_sums(element):
            (user_ac, count) = element
            return {'user_activity': user_ac, 'activity_count': count}

        # Get team scores and write the results to the file system
        (  # pylint: disable=expression-not-assigned
                events
                | 'CalculateActivityCount' >> CalculateActivityCount()
                | 'Format' >> beam.Map(format_user_activity_sums)
                | 'WriteTeamScoreSums' >> beam.io.WriteToText(args.output1))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
