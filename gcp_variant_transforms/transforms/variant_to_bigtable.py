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

import exceptions
import random
import re
from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam
from google.cloud import bigtable

from gcp_variant_transforms.libs import processed_variant


# TODO before submit: These should come from flags.
_PROJECT_ID = 'bashir-variant'
_INSTANCE_ID = 'variant-test'
_TABLE_NAME = 'platinum_all_more_nodes_more_workers'
_SAMPLES_FAMILY = 'samples_family'

@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class _WriteToBigTable(beam.DoFn):
  """Writes a ``Variant`` record to a BigTable."""

  def __init__(self,
               allow_incompatible_records=False,
               omit_empty_sample_calls=False):
    # type: (bool, bool) -> None
    super(_WriteToBigTable, self).__init__()
    # TODO remove the rest before submit if not needed
    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls

  def process(self, variant):
    # type: (processed_variant.ProcessedVariant) -> None
    row_key = self._make_key(variant)
    row = self._table.row(row_key)
    for call in variant.calls:
      row.set_cell(_SAMPLES_FAMILY, call.name, str(call.genotype))
    row.commit()

  def _make_key(self, variant):
    # type: (processed_variant.ProcessedVariant) -> str
    # TODO(bashir2): Extend this to include reference sequence ID too and do
    # normalization as well.
    return '{}:{}'.format(variant.reference_name, variant.start)

  def start_bundle(self):
    # TODO before submit: Remove unnecessary instant variables.
    self._bt_client = bigtable.Client(project=_PROJECT_ID, admin=True)
    self._bt_instance = self._bt_client.instance(_INSTANCE_ID)
    self._table = self._bt_instance.table(_TABLE_NAME)


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class VariantToBigTable(beam.PTransform):
  """Writes PCollection of `ProcessedVariant` records to BigTable."""

  def __init__(
      self,
      output_table,  # type: str
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False,  # type: bool
      ):
    # type: (...) -> None
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      header_fields: Representative header fields for all variants. This is
        needed for dynamically generating the schema.
      allow_incompatible_records: If true, field values are casted to Bigquery
        schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
    """
    self._output_table = output_table
    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls
    client = bigtable.Client(project=_PROJECT_ID, admin=True)
    instance = client.instance(_INSTANCE_ID)
    #table = instance.table(_TABLE_NAME)
    #table.create()
    #cf = table.column_family(_SAMPLES_FAMILY)
    #cf.create()

  def expand(self, pcoll):
    return pcoll | 'WriteToBigTable' >> beam.ParDo(
        _WriteToBigTable(
            self._allow_incompatible_records,
            self._omit_empty_sample_calls))

