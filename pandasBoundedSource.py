#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

import argparse
import logging
import re


import apache_beam as beam
from apache_beam.metrics import Metrics as metrics
from apache_beam.io import iobase, range_trackers

"""
PandasSource will select bundles of data from a dataframe, to read it's rows in parallel
"""

class PandaSource(iobase.BoundedSource):
    def __init__(self, dataframe):
        self.records_read = metrics.counter(self.__class__, 'recordsRead')
        self._dataframe = dataframe
    
    def estimate_size(self):
        return len(self._dataframe.index)

    def split(self, desired_bundle_size, start_position=None, stop_position=None):

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = len(self._dataframe.index)

        bundle_start = start_position
        while bundle_start < len(self._dataframe.index):
            bundle_stop = max(len(self._dataframe.index), bundle_start + desired_bundle_size)
            yield iobase.SourceBundle(weight=(bundle_stop - bundle_start), source=self, start_position = bundle_start, stop_position = bundle_stop)
            bundle_start = bundle_stop
    
    def get_range_tracker(self, start_position, stop_position):

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = len(self._dataframe.index)
        
        return range_trackers.OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        for i in range(len(self._dataframe.index)):
            if not range_tracker.try_claim(i):
                return
                
            self.records_read.inc()
            yield self._dataframe.iloc[i]

#Usage example
"""with beam.Pipeline(options=PipelineOptions()) as p:
    rows = (
        p | 
        beam.io.Read(PandaSource(dataframe))
    )"""

