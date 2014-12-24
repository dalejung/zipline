
#
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tools to generate data sources.
"""
import itertools
from collections import OrderedDict

import pandas as pd

from zipline.gens.utils import hash_args

from zipline.sources.data_source import DataSource


class DataFrameSource(DataSource):
    """
    Yields all events in event_list that match the given sid_filter.
    If no event_list is specified, generates an internal stream of events
    to filter.  Returns all events if filter is None.

    Configuration options:

    sids   : list of values representing simulated internal sids
    start  : start date
    delta  : timedelta between internal events
    filter : filter to remove the sids
    """

    def __init__(self, data, **kwargs):
        assert isinstance(data.index, pd.tseries.index.DatetimeIndex)

        self.data = data
        # Unpack config dictionary with default values.
        self.sids = kwargs.get('sids', data.columns)
        self.start = kwargs.get('start', data.index[0])
        self.end = kwargs.get('end', data.index[-1])

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(data, **kwargs)

        self._raw_data = None

    @property
    def mapping(self):
        return {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
        }

    @property
    def instance_hash(self):
        return self.arg_string

    def raw_data_gen(self):
        for dt, series in self.data.iterrows():
            for sid, price in series.iteritems():
                if sid in self.sids:
                    event = {
                        'dt': dt,
                        'sid': sid,
                        'price': price,
                        'volume': 1000,
                    }
                    yield event

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data


class SourceEvent(object):
    # using slots to keep user variables separate
    __slots__ = ['_values', '_index', '_mapping', '_userdict']

    def __init__(self, values, index, mapping, **kwargs):
        self._userdict = kwargs
        self._values = values
        self._index = index
        self._mapping = mapping

    def __getattr__(self, name):
        try:
            return self._userdict[name]
        except KeyError:
            pass

        try:
            # get source attr
            i = self._index[name]
        except KeyError:
            raise AttributeError(name)

        val = self._values[i]
        mapper = self._mapping.get(name, None)
        if mapper:
            mapping_func, source_key = mapper
            val = mapping_func(val)
        return val

    def __setattr__(self, name, value):
        if name == '_userdict':
            object.__setattr__(self, name, value)
            return

        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return

        # don't allow panel source attrs to change
        if hasattr(self, '_index') and name in self._index:
            raise Exception("{name}: Cannot change a source attr"
                            .format(name=name))
        self._userdict[name] = value

    def __delattr__(self, name):
        if name in self._index:
            raise Exception("{name}: Cannot delete a source attr"
                            .format(name=name))
        del self._userdict[name]

    def __contains__(self, name):
        return hasattr(self, name)

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def __delitem__(self, name, value):
        delattr(self, name, value)

    @property
    def __dict__(self):
        # this replicate the old Event __dict__
        # TODO: should cache and make frozen.
        dct = {k: getattr(self, k) for k in self._index}
        dct.update(self._userdict)
        return dct

    def keys(self):
        return itertools.chain(self._userdict, self._index)

    def __repr__(self):
        vals = {k: getattr(self, k) for k in self._index}
        vals.update(self._userdict)
        return "SourceEvent({0})".format(vals)

    def to_series(self, index=None):
        if index is None:
            index = self._index
        return pd.Series(self._values, index=index)


class DataPanelSource(DataSource):
    """
    Yields all events in event_list that match the given sid_filter.
    If no event_list is specified, generates an internal stream of events
    to filter.  Returns all events if filter is None.

    Configuration options:

    sids   : list of values representing simulated internal sids
    start  : start date
    delta  : timedelta between internal events
    filter : filter to remove the sids
    """

    def __init__(self, data, **kwargs):
        assert isinstance(data.major_axis, pd.tseries.index.DatetimeIndex)

        self.data = data
        # Unpack config dictionary with default values.
        self.sids = kwargs.get('sids', data.items)
        self.start = kwargs.get('start', data.major_axis[0])
        self.end = kwargs.get('end', data.major_axis[-1])

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(data, **kwargs)

        self._raw_data = None

    @property
    def mapping(self):
        mapping = {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
        }

        # Add additional fields.
        for field_name in self.data.minor_axis:
            if field_name in ['price', 'volume', 'dt', 'sid']:
                continue
            mapping[field_name] = (lambda x: x, field_name)

        return mapping

    @property
    def instance_hash(self):
        return self.arg_string

    def raw_data_gen(self):
        values = self.data.values
        major_axis = self.data.major_axis
        minor_axis = self.data.minor_axis
        minor_axis = OrderedDict(zip(minor_axis, range(len(minor_axis))))
        items = self.data.items

        source_id = self.get_hash()
        event_type = self.event_type
        mapping = self.mapping

        for i, dt in enumerate(major_axis):
            df = values[:, i, :]
            for k, sid in enumerate(items):
                if sid in self.sids:
                    series = df[k]
                    yield SourceEvent(series, minor_axis, mapping,
                                      source_id=source_id, type=event_type,
                                      sid=sid, dt=dt)

    @property
    def mapped_data(self):
        for row in self.raw_data:
            yield row

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data
