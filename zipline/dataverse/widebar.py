from zipline.dataverse.dataverse import BaseDataverse
import zipline.protocol as zp


class WidebarSIDData(zp.SIDData):
    def __init__(self, *args, **kwargs):
        dataverse = kwargs['dataverse']  # do not pop
        self._columns = dataverse.trade_columns
        self._columns_set = set(self._columns)
        super(WidebarSIDData, self).__init__(*args, **kwargs)

    def update(self, *args, **kwargs):
        assert not self._columns_set.intersection(kwargs)
        self.__dict__.update(*args, **kwargs)

    def __getattr__(self, name):
        if name in self._columns_set:
            return self._dataverse.get_sid_data(self._sid, name)
        raise AttributeError("SIDData missing {name}".format(name=name))

    @property
    def dt(self):
        return self._dataverse.dt

    def __setattr__(self, name, value):
        return super(WidebarSIDData, self).__setattr__(name, value)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def __getitem__(self, name):
        return getattr(self, name)

    def __contains__(self, name):
        return name in self.__dict__ or name in self._columns_set

    def __len__(self):
        return len(self._columns) + len(self.__dict__) - self._initial_len


class WidebarDataverse(BaseDataverse):
    """
    Dataverse that uses HistoryChunker
    """

    siddata_class = WidebarSIDData

    def __init__(self):
        self.current_event = None
        super(WidebarDataverse, self).__init__()

    def asset_finder(self, sid):
        return sid

    def get_source(self, source, overwrite_sim_params=True):
        self.raw_source = source
        source = super(WidebarDataverse, self).get_source(source)
        self.source = source
        return source

    def update_universe_trade(self, event):
        # we do not update universe i.e. BarData
        self.current_event = event
        # populate the missing entries
        sid_translate = getattr(event, 'sid_translate', {})
        self.current_data.populate_sids(event.sids_set, sid_translate)

    def get_sid_data(self, sid, name):
        current_event = self.current_event
        sid_loc = current_event.sids.get_loc(sid)
        col_loc = current_event.columns.get_loc(name)
        return current_event.values[sid_loc, col_loc]

    @property
    def trade_columns(self):
        return self.current_event.columns
