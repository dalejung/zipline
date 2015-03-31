from zipline.dataverse.dataverse import BaseDataverse

from .history import BacktestHistoryContainer
from .bardata import BacktestBarData, BacktestSIDData

from zipline.utils.munge import ffill


class HistoryDataverse(BaseDataverse):
    """
    Dataverse that uses HistoryChunker
    """
    history_container_class = BacktestHistoryContainer

    def __init__(self):
        self.current_data = self.bardata_class(dataverse=self)
        self.datetime = None

    def asset_finder(self, sid):
        return sid

    def get_history_container(self, *args, **kwargs):
        kwargs['dataverse'] = self
        return self.history_container_class(*args, **kwargs)

    def get_source(self, source, overwrite_sim_params=True):
        self.raw_source = source
        source = super(HistoryDataverse, self).get_source(source)
        self.source = source
        return source

    def on_dt_changed(self, dt):
        self.datetime = dt


class BacktestDataverse(BaseDataverse):
    """
    Precog. This dataverse is a work in progress
    """
    history_container_class = BacktestHistoryContainer
    siddata_class = BacktestSIDData
    bardata_class = BacktestBarData

    def __init__(self):
        self.current_data = self.bardata_class(dataverse=self)
        self.datetime = None

    def asset_finder(self, sid):
        return sid

    def get_history_container(self, *args, **kwargs):
        kwargs['dataverse'] = self
        return self.history_container_class(*args, **kwargs)

    def get_source(self, source, overwrite_sim_params=True):
        self.raw_source = source
        source = super(BacktestDataverse, self).get_source(source)
        self.source = source
        return source

    def update_universe(self, event):
        # we do not update universe i.e. BarData
        pass

    def pre_simulation(self):
        # prime the BarData class with the proper sids
        for sid in self.source.sids:
            sid = self.asset_finder(sid)
            self.current_data.get_default(sid)

    def on_dt_changed(self, dt):
        self.datetime = dt

    def get_sid_data(self, sid, name):
        pass


class DataverseChunk(object):
    """
    """
    def __init__(self, values, index, window, ffill=True, last_values=None):
        self.values = values
        self.index = index
        self.ffill = ffill
        self.last_values = last_values

    def __iter__(self):
        """
        Coroutine to generate ndarray slices.
        """
        values = self.values
        index = self.index
        get_loc = index.get_loc

        if self.ffill:
            values = ffill(values)

        vals = None
        while True:
            dt = (yield vals)
            # TODO walk a starting loc_index to limit search space.
            # in most cases the next loc will be right after the last one
            loc = get_loc(dt)
            start = max(loc - self.window, 0)
            sl = slice(start, loc)
            vals = values[sl]
