# Copyright 2017 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A PTransform to output a PCollection of ``Variant`` records to BigQuery."""

from __future__ import absolute_import

import argparse
import logging

from google.cloud import bigtable


_PROJECT_ID = 'bashir-variant'
_INSTANCE_ID = 'variant-test'


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--table_name', default='',
                      help='The name of the BigTable table to be queried.')
  parser.add_argument('--family_name', default='',
                      help='The name of the column family to read.')
  parser.add_argument('--ranges_file', default='',
                      help='The name of the file containing range values.')
  parser.add_argument('--mode', default='single',
                      help='Either read "single" rows or "range" of rows.')
  known_args, remaining_args = parser.parse_known_args()
  if remaining_args:
    print 'Unexpected arguments {}'.format(remaining_args)
    exit()
  return known_args


def read_ranges(filename):
  file = open(filename, 'r')
  for line in file:
    r = line.split()
    yield r[0], r[1]


def run():
  args = parse_args()
  bt_client = bigtable.Client(project=_PROJECT_ID, admin=True)
  bt_instance = bt_client.instance(_INSTANCE_ID)
  table = bt_instance.table(args.table_name)

  n = 0
  n_rows = 0
  n_cells = 0
  for range in read_ranges(args.ranges_file):
    if args.mode is 'single':
      partial_rows = table.read_rows(start_key='chr1#{}'.format(range[0]),
                                     end_key='chr1#{}#'.format(range[0]))
    else:
      partial_rows = table.read_rows(start_key='chr1#{}'.format(range[0]),
                                     end_key='chr1#{}'.format(range[1]))
    n += 1
    partial_rows.consume_all()
    for row_key, row in partial_rows.rows.items():
      cells_dict = row.to_dict()
      key = row_key.decode('utf-8')
      n_rows += 1
      n_cells += len(cells_dict)
      #print 'HERE {} {}'.format(key, len(cells_dict))
      #for column, cells in cells_dict.iteritems():
      #  print 'HERE key: {} column: {} value: {}'.format(key, column,
      #                                                   cells[0].value)
  print 'HERE n= {}  n_rows= {} n_cells= {}'.format(n, n_rows, n_cells)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
