import functools


class memoized(object):
    """Decorator that caches a function's return value each time it is called.

    Notes:
    Only args are allowed.
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        try:
            return self.cache[args]
        except KeyError:
            value = self.func(*args)
            self.cache[args] = value
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(*args)

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


@memoized
def fibonacci(n):
    """Example: return the nth fibonacci number."""
    if n in (0, 1):
        return n

    return fibonacci(n-1) + fibonacci(n-2)


if __name__ == "__main__":
    print fibonacci(120)
    print fibonacci(120)
