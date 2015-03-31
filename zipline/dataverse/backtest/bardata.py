import zipline.protocol as zp


class BacktestSIDData(object):
    def __init__(self, sid, dataverse, initial_values=None):
        self.sid = sid
        self.dataverse = dataverse
        self.obj = zp.SIDData(sid)
        if initial_values:
            self.obj.update(initial_values)

    def update(self, *args, **kwargs):
        return self.obj.update(*args, **kwargs)

    @property
    def datetime(self):
        return self.obj.dt

    def __getattr__(self, name):
        try:
            return self.dataverse.get_sid_data(self.sid, name)
        except:
            return getattr(self.obj, name)

    def get(self, name, default=None):
        return self.obj.get(name, default)

    def __getitem__(self, name):
        return self.obj[name]

    def __setitem__(self, name, value):
        self.obj[name] = value

    def __len__(self):
        # hack for now. Need to figure out how the magic SIDData will
        # handle this
        return max(len(self.obj), 1)

    def __contains__(self, name):
        return name in self.obj

    def __repr__(self):
        return repr(self.obj)

    def mavg(self, days):
        return self.obj.mavg(days)

    def stddev(self, days):
        return self.obj.stddev(days)

    def vwap(self, days):
        return self.obj.vwap(days)

    def returns(self):
        return self.obj.returns()


class BacktestBarData(zp.BarData):
    def __init__(self, data=None, siddata_class=BacktestSIDData,
                 dataverse=None):
        if dataverse is None:
            raise TypeError("dataverse cannot be None for BacktestBarData")
        self._dataverse = dataverse
        self._data = data or {}
        self._contains_override = None
        self._siddata_class = siddata_class
