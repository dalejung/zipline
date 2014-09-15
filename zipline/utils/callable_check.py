from collections import namedtuple
import inspect
from itertools import starmap, izip_longest, chain, takewhile
import operator

from zipline.errors import ZiplineError


class Argument(namedtuple('Argument', ['name', 'default'])):
    no_default = object()  # sentinel value
    ignore = object()  # sentinel value

    def __new__(cls, name, default=ignore):
        return super(Argument, cls).__new__(cls, name, default)

    def __str__(self):
        if self.has_no_default(self) or self.ignore_default(self):
            return self.name
        else:
            return '='.join([self.name, str(self.default)])

    def __eq__(self, arg):
        return arg.name == self.name \
            and (arg.default == self.default
                 or Argument.ignore in [self.default, arg.default])

    @staticmethod
    def has_no_default(arg):
        return arg.default is Argument.no_default

    @staticmethod
    def ignore_default(arg):
        return arg.default is Argument.ignore


def callable_check(callable_,
                   expected_args=Argument.ignore,
                   expect_starargs=Argument.ignore,
                   expect_kwargs=Argument.ignore):
    """
    Checks the callable_ to make sure that it satisfies the given
    expectations.
    expected_args should be an iterable of Arguments.
    """
    if not callable(callable_):
        raise NotCallable(callable_)

    expected_arg_list = list(
        expected_args if expected_args is not Argument.ignore else []
    )

    argspec = inspect.getargspec(callable_)
    args, starargs, kwargs = BadCallable._parse_argspec(argspec, callable_)

    if starargs:
        if not expect_starargs:
            raise UnexpectedStarargs(callable_, args, starargs, kwargs)
    elif expect_starargs and expect_starargs is not Argument.ignore:
        raise NoStarargs(callable_, args, starargs, kwargs)

    if kwargs:
        if not expect_kwargs:
            raise UnexpectedKwargs(callable_, args, starargs, kwargs)
    elif expect_kwargs and expect_kwargs is not Argument.ignore:
        raise NoKwargs(callable_, args, starargs, kwargs)

    if expected_args is Argument.ignore:
        # Ignore the argument list checks.
        return

    missing_args = [arg for arg in expected_arg_list if arg not in args]
    if missing_args:
        raise NotEnoughArgmuments(
            callable_, args, starargs, kwargs, missing_args
        )

    if len(args) < len(expected_arg_list):
        raise NotEnoughArgmuments(
            callable_, args, starargs, kwargs
        )

    missing_arg = Argument(object(), object())
    num_correct = len(
        list(
            takewhile(
                lambda a: a,
                starmap(
                    operator.eq,
                    izip_longest(
                        expected_arg_list,
                        args,
                        fillvalue=missing_arg,
                    ),
                ),
            ),
        ),
    )
    if num_correct != len(args):
        raise TooManyArguments(
            callable_, args, starargs, kwargs
        )


class BadCallable(ZiplineError, TypeError, AssertionError):
    """
    The given callable is not structured in the expected way.
    """

    def __init__(self, callable_, args, starargs, kwargs):
        self.callable_ = callable_
        self.args = args
        self.starargs = starargs
        self.kwargs = kwargs

    @staticmethod
    def _parse_argspec(argspec, callable_):
        args, varargs, keywords, defaults = argspec
        defaults = list(defaults or [])

        if getattr(callable_, 'im_self', None) is not None:
            # This is a bound method, drop the self param.
            args = args[1:]

        return list(reversed(list(starmap(
            Argument,
            izip_longest(reversed(args), defaults,
                         fillvalue=Argument.no_default)
        )))), varargs, keywords

    def format_callable(self):
        return '%s(%s)' % (
            self.callable_.__name__,
            ', '.join(
                chain(
                    (str(arg) for arg in self.args),
                    ('*' + sa for sa in (self.starargs,) if sa is not None),
                    ('**' + ka for ka in (self.kwargs,) if ka is not None),
                )
            )
        )


class NoStarargs(BadCallable):
    def __str__(self):
        return '%s does not allow for *args' % self.format_callable()


class UnexpectedStarargs(BadCallable):
    def __str__(self):
        return '%s should not allow for *args' % self.format_callable()


class NoKwargs(BadCallable):
    def __str__(self):
        return '%s does not allow for **kwargs' % self.format_callable()


class UnexpectedKwargs(BadCallable):
    def __str__(self):
        return '%s should not allow for **kwargs' % self.format_callable()


class NotCallable(BadCallable):
    """
    The provided 'callable' is not actually a callable.
    """
    def __init__(self, callable_):
        self.callable_ = callable_

    def __str__(self):
        return '%s is not callable' % self.format_callable()

    def format_callable(self):
        try:
            return self.callable_.__name__
        except AttributeError:
            return str(self.callable_)


class NotEnoughArgmuments(BadCallable):
    """
    The callback does not accept enough arguments.
    """
    def __init__(self, callable_, args, starargs, kwargs, missing_args):
        super(NotEnoughArgmuments, self).__init__(
            callable_, args, starargs, kwargs
        )
        self.missing_args = missing_args

    def __str__(self):
        missing_args = map(str, self.missing_args)
        return '%s is missing argument%s: %s' % (
            self.format_callable(),
            's' if len(missing_args) > 1 else '',
            ', '.join(missing_args),
        )


class TooManyArguments(BadCallable):
    """
    The callback cannot be called by passing the expected positional arguments.
    """
    def __str__(self):
        return '%s accepts too many arguments' % self.format_callable()
