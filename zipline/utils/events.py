from abc import ABCMeta, abstractmethod
from collections import namedtuple
from functools import partial
import operator
import six

import datetime
import pandas as pd
import pytz

from zipline.finance.trading import TradingEnvironment
from zipline.utils.callable_check import callable_check, Argument


ENV = TradingEnvironment.instance()

# An empty time delta.
no_offset = datetime.timedelta()


def naive_to_utc(ts):
    """
    Converts a UTC tz-naive timestamp to a tz-aware timestamp.
    """
    # Drop the nanoseconds field. warn=False suppresses the warning
    # that we are losing the nanoseconds; however, this is intended.
    return pd.Timestamp(ts.to_pydatetime(warn=False), tz='UTC')


def normalize_utc(time, tz='UTC'):
    """
    Normalize a time. If the time is tz-naive, assume it is UTC.
    """
    if not time.tzinfo:
        time = time.tznormalize(pytz.timezone(tz))
    return time.tzconvert(pytz.utc)


def _build_offset(offset, kwargs):
    """
    Builds the offset argument for event rules.
    """
    if offset is None:
        if not kwargs:
            return no_offset
        else:
            return datetime.timedelta(**kwargs)
    elif kwargs:
        raise ValueError('Cannot pass kwargs and an offset')
    else:
        return offset


def _build_date(date, kwargs):
    """
    Builds the date argument for event rules.
    """
    if date is None:
        if not kwargs:
            raise ValueError('Must pass a date or kwargs')
        else:
            return datetime.date(**kwargs)

    elif kwargs:
        raise ValueError('Cannot pass kwargs and a date')
    else:
        return pd.tslib.normalize_date(date)


def _build_time(time, kwargs):
    """
    Builds the time argument for event rules.
    """
    tz = kwargs.pop('tz', 'UTC')
    if time:
        if kwargs:
            raise ValueError('Cannot pass kwargs and a time')
        else:
            return normalize_utc(time, tz)
    elif not kwargs:
        raise ValueError('Must pass a time or kwargs')
    else:
        return datetime.time(**kwargs)


class EventManager(object):
    """
    Manages a list of Event objects.
    This manages the logic for checking the rules and dispatching to the
    handle_data function of the Events.
    """
    def __init__(self):
        self._events = []

    def add_event(self, event):
        """
        Adds an event to the manager.
        """
        self._events.append(event)

    def handle_data(self, context, data, dt):
        for event in self._events:
            event.handle_data(context, data, dt)


class Event(namedtuple('Event', ['rule', 'callback'])):
    """
    An event is a pairing of an EventRule and a callable that will be invoked
    with the current algorithm context, data, and datetime only when the rule
    is triggered.
    """
    def __new__(cls, rule=None, callback=None):
        callback = callback or (lambda *args, **kwargs: None)

        # Check the callback provided.
        callable_check(
            callback,
            [Argument('context'), Argument('data')]
        )
        # Make sure that the rule's should_trigger is valid. This will catch
        # potential errors much more quickly and give a more helpful error.
        callable_check(getattr(rule, 'should_trigger'), [Argument('dt')])

        return super(cls, cls).__new__(cls, rule=rule, callback=callback)

    def handle_data(self, context, data, dt):
        """
        Calls the callable only when the rule is triggered.
        """
        if self.rule.should_trigger(dt):
            self.callback(context, data)


class EventRule(six.with_metaclass(ABCMeta)):
    """
    An event rule checks a datetime and sees if it should trigger.
    """
    @abstractmethod
    def should_trigger(self, dt):
        """
        Checks if the rule should trigger with it's current state.
        This method should be pure and NOT mutate any state on the object.
        """
        raise NotImplementedError('should_trigger')


class StatelessRule(EventRule):
    """
    A stateless rule has no state.
    This is reentrant and will always give the same result for the
    same datetime.
    Because these are pure, they can be composed to create new rules.
    """
    def and_(self, rule):
        """
        Logical and of two rules, triggers only when both rules trigger.
        This follows the short circuiting rules for normal and.
        """
        return ComposedRule(self, rule, ComposedRule.lazy_and, lazy=True)
    __and__ = and_

    def or_(self, rule):
        """
        Logical or of two rules, triggers when either rule triggers.
        This follows the short circuiting rules for normal or.
        """
        return ComposedRule(self, rule, ComposedRule.lazy_or, lazy=True)
    __or__ = or_

    def xor(self, rule):
        """
        Logical xor of two rules, triggers if exactly one rule is triggered.
        """
        return ComposedRule(self, rule, operator.xor)
    __xor__ = xor

    def invert(self):
        """
        Logical inversion of a rule, triggers only when the rule is not
        triggered.
        """
        return InvertedRule(self)
    __invert__ = invert


# Stateless Rules

class InvertedRule(StatelessRule):
    """
    A rule that inverts the results of another rule.
    """
    def __init__(self, rule):
        if not isinstance(rule, StatelessRule):
            raise ValueError('Only StatelessRules can be inverted')
        self.rule = rule

    def should_trigger(self, dt):
        """
        Triggers only when self.rule.should_trigger(dt) does not trigger.
        """
        return not self.rule.should_trigger(dt)


class ComposedRule(StatelessRule):
    """
    A rule that composes the results of two rules with some composing function.
    The composing function should be a binary function that accepts the results
    first(dt) and second(dt) as positional arguments.
    For example, operator.and_.
    If lazy=True, then the lazy composer is used instead. The lazy composer
    expects a function that takes the two should_trigger functions and the
    datetime. This is useful of you don't always want to call should_trigger
    for one of the rules. For example, this is used to implement the & and |
    operators so that they will have the same short circuit logic that is
    expected.
    """
    def __init__(self, first, second, composer, lazy=False):
        if not (isinstance(first, StatelessRule)
                and isinstance(second, StatelessRule)):
            raise ValueError('Only two StatelessRules can be composed')

        self.first = first
        self.second = second
        self.composer = composer
        if lazy:
            # Switch the the lazy should trigger instead.
            self.should_trigger = self._lazy_should_trigger

    def should_trigger(self, dt):
        """
        Composes the results of two rule's should_trigger methods to get the
        result for this rule.
        """
        return self.composer(
            self.first.should_trigger(dt),
            self.second.should_trigger(dt),
        )

    def _lazy_should_trigger(self, dt):
        """
        Composes the two rules with a lazy composer. This is used when
        lazy=True in __init__.
        """
        return self.composer(
            self.first.should_trigger,
            self.second.should_trigger,
            dt,
        )

    @staticmethod
    def lazy_and(first_should_trigger, second_should_trigger, dt):
        """
        Lazily ands the two rules. This will NOT call the should_trigger of the
        second rule if the first one returns False.
        """
        return first_should_trigger(dt) and second_should_trigger(dt)

    @staticmethod
    def lazy_or(first_should_trigger, second_should_trigger, dt):
        """
        Lazily ors the two rules. This will NOT call the should_trigger of the
        second rule the first one returns True.
        """
        return first_should_trigger(dt) or second_should_trigger(dt)


class Always(StatelessRule):
    """
    A rule that always triggers.
    """
    @staticmethod
    def always_trigger(dt):
        """
        A should_trigger implementation that will always trigger.
        """
        return True
    should_trigger = always_trigger


class Never(StatelessRule):
    """
    A rule that never triggers.
    """
    @staticmethod
    def never_trigger(dt):
        """
        A should_trigger implementation that will never trigger.
        """
        return False
    should_trigger = never_trigger


class AfterOpen(StatelessRule):
    """
    A rule that triggers for some offset after the market opens.
    Example that triggers triggers after 30 minutes of the market opening:

    >>> AfterOpen(minutes=30)
    """
    def __init__(self, offset=None, **kwargs):
        self.offset = _build_offset(offset, kwargs)

    def should_trigger(self, dt):
        return ENV.get_open_and_close(dt)[0] + self.offset >= dt


class BeforeClose(StatelessRule):
    """
    A rule that triggers for some offset time before the market closes.
    Example that triggers for the last 30 minutes every day:

    >>> BeforeClose(minutes=30)
    """
    def __init__(self, offset=None, **kwargs):
        self.offset = _build_offset(offset, kwargs)

    def should_trigger(self, dt):
        return ENV.get_open_and_close(dt)[1] - self.offset < dt


class OnDate(StatelessRule):
    """
    A rule that triggers on a certain date.
    """
    def __init__(self, date=None, **kwargs):
        self.date = _build_date(date, kwargs)

    def should_trigger(self, dt):
        return pd.tslib.normalize_date(dt) == self.date


class AfterDate(StatelessRule):
    """
    A rule that triggers after a certain date.
    """
    def __init__(self, date, **kwargs):
        self.date = _build_date(date, kwargs)

    def should_trigger(self, dt):
        return pd.tslib.normalize_date(dt) > self.date


class BeforeDate(StatelessRule):
    """
    A rule that triggers before a certain date.
    """
    def __init__(self, date, **kwargs):
        self.date = _build_date(date, kwargs)

    def should_trigger(self, dt):
        return pd.tslib.normalize_date(dt) < self.date


def BetweenDates(date1, date2):
    """
    A rule that triggers between in the range [date1, date2).
    """
    return (OnDate(date1) | AfterDate(date1)) & BeforeDate(date2)


class AtTime(StatelessRule):
    """
    A rule that triggers at an exact time.
    """
    def __init__(self, time=None, **kwargs):
        self.time = _build_time(time, kwargs)

    def should_trigger(self, dt):
        return dt.timetz() == self.time


class AfterTime(StatelessRule):
    """
    A rule that triggers after a given time.
    """
    def __init__(self, time=None, **kwargs):
        self.time = _build_time(time, kwargs)

    def should_trigger(self, dt):
        return dt.timetz() > self.time


class BeforeTime(StatelessRule):
    """
    A rule that triggers before a given time.
    """
    def __init__(self, time=None, **kwargs):
        self.time = _build_time(time, kwargs)

    def should_trigger(self, dt):
        return dt.timetz() < self.time


def BetweenTimes(time1=None, time2=None, tz='UTC'):
    """
    A rule that triggers when the datetime is in the range [time1, time2).
    """
    return (AtTime(time1, tz=tz)
            | AfterTime(time1, tz=tz)) & BeforeTime(time2, tz=tz)


class FirstTradingDayOfMonth(StatelessRule):
    """
    A rule that triggers on the first trading day of every month.
    """
    def __init__(self):
        self.month = None
        self.first_day = None

    def should_trigger(self, dt):
        return self.get_first_trading_day_of_month(dt) == dt.date()

    def get_first_trading_day_of_month(self, dt):
        """
        If the first trading day for this month is computed, then return the
        cached value, otherwise, compute and cache the first trading day of the
        month.
        """
        if self.month == dt.month:
            # We already computed the first day for this month.
            return self.first_day

        self.month = dt.month

        dt = dt.replace(day=1)
        self.first_day = (dt if ENV.is_trading_day(dt)
                          else ENV.next_trading_day(dt)).date()
        return self.first_day


class LastTradingDayOfMonth(StatelessRule):
    """
    A rule that triggers on the last trading day of every month.
    """
    def __init__(self):
        self.month = None
        self.last_day = None

    def should_trigger(self, dt):
        return self.get_last_trading_day_of_month(dt) == dt.date()

    def get_last_trading_day_of_month(self, dt):
        """
        If the last trading day for this month is computed, then return the
        cached value, otherwise, compute and cache the first trading day of the
        month.
        """
        if self.month == dt.month:
            # We already computed the last day for this month.
            return self.last_day

        self.month = dt.month

        self.last_day = ENV.previous_trading_day(
            dt.replace(month=(dt.month % 12) + 1, day=1)
        ).date()
        return self.last_day


# Stateful rules


class StatefulRule(EventRule):
    """
    A stateful rule has state.
    This rule will give different results for the same datetimes depending
    on the internal state that this holds.
    StatefulRules wrap other rules as state transformers.
    """
    def __init__(self, rule=None):
        self.rule = rule or Always()

    def new_should_trigger(self, callable_):
        """
        Replace the should trigger implementation for the current rule.
        """
        self.should_trigger = callable_


class RuleFromCallable(StatefulRule):
    """
    Constructs an EventRule from an arbitrary callable.
    """
    def __init__(self, callback, rule=None):
        """
        Constructs an EventRule from a callable.
        If the provided paramater is not callable, or cannot be called with a
        single paramater, then a ValueError will be raised.
        """
        # Check that callback meets the criteria for a rule's should_trigger.
        callable_check(callback, [Argument('dt')])

        self.callback = callback

        super(RuleFromCallable, self).__init__(rule)

    def should_trigger(self, dt):
        """
        Only check the wrapped should trigger when callback is true.
        """
        return self.callback(dt) and self.rule.should_trigger(dt)


class Once(StatefulRule):
    """
    A rule that triggers only once.
    """
    def should_trigger(self, dt):
        triggered = self.rule.should_trigger(dt)
        if triggered:
            self.new_should_trigger(Never.never_trigger)
        return triggered


class DoNTimes(StatefulRule):
    """
    A rule that triggers n times.
    """
    def __init__(self, n, rule=None):
        self.n = n
        super(DoNTimes, self).__init__(rule)

    def should_trigger(self, dt):
        """
        Only triggers n times before switching to a never_trigger mode.
        """
        triggered = self.rule.should_trigger(dt)
        if triggered:
            self.n -= 1
            if self.n == 0:
                self.new_should_trigger(Never.never_trigger)
            return True
        return False


class SkipNTimes(StatefulRule):
    """
    A rule that skips the first n times.
    """
    def __init__(self, n, rule=None):
        self.n = n
        super(SkipNTimes, self).__init__(rule)

    def should_trigger(self, dt):
        """
        Skips the first n times before switching to the inner rule's
        should_trigger.
        """
        triggered = self.rule.should_trigger(dt)
        if triggered:
            self.n -= 1
            if self.n == 0:
                self.new_should_trigger(self.rule.should_trigger)
        return False


class NTimesPerPeriod(StatefulRule):
    """
    A rule that triggers n times in a given period.
    """
    def __init__(self, n=1, freq='B', rule=None):
        self.n = n
        self.freq = freq
        self.end_time = pd.Period(ENV.first_trading_day, freq=freq) \
                          .end_time.tz_localize('UTC')
        self.end_time.nanoseconds = 0
        self.hit_this_period = 0
        super(NTimesPerPeriod, self).__init__(rule)

    def should_trigger(self, dt):
        if dt < self.end_time:
            if self.rule.should_trigger(dt):
                self.hit_this_period += 1
                return self.hit_this_period <= self.n
            else:
                return False
        else:
            # We are now in the next period, compute the next period end and
            # reset the counter.
            self.end_time = naive_to_utc(
                pd.Period(dt, freq=self.freq).end_time
            )
            self.hit_this_period = 1
            return self.hit_this_period <= self.n


# Convenience aliases for common use cases on NTimesPerPeriod.
OncePerPeriod = partial(NTimesPerPeriod, n=1)
OncePerDay = partial(NTimesPerPeriod, n=1, freq='B')
OncePerWeek = partial(NTimesPerPeriod, n=1, freq='W')
OncePerMonth = partial(NTimesPerPeriod, n=1, freq='M')
OncePerQuarter = partial(NTimesPerPeriod, n=1, freq='Q')
