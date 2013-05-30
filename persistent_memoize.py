import cPickle as pickle
import hashlib
import random
import time
import shelve
import functools
import os
import utils
import re


def _make_serializable(item):
    """repackage an object so that it can be pickle.dumps()"""
    return item


memoize_directory = "./"


def memoize_persistent(func):
    """Memoize with a persistent cache.

    Notes:
    The cache files are uniquely specified with a SHA224 hash based on their
    arguments and function call name.

    Keyword arguments are an unsorted dict; sort them to memoize consistently

    All inputs/outputs must be serializable with protocol=-1; otherwise,
    write a wrapper.

    This is designed to work with multiple processes. Procedure:
    If the requested result does not exist, or is being calculated, write a
    lock ".busy" file and begin the calculation, save it to a new file,
    and return the result.

    If the requested result exists, read the file and return it.

    If the requested results is being calculated in another process/thread,
    wait until it finishes; load the output and return it.

    TODO:
    better thread safety or SQLite?
    """
    def memoize(*args, **kwargs):
        funcname = func.__name__
        rehashed = [_make_serializable(item) for item in args]

        argpkl = pickle.dumps((funcname, rehashed,
                               tuple(sorted(kwargs.items()))), -1)

        identifier = hashlib.sha224(argpkl).hexdigest()

        readable = utils.readable_call(funcname, rehashed, kwargs)
        filename = "%s/%s.shelve" % (memoize_directory, identifier)
        filename = re.sub('/+', '/', filename)
        print "%s -> %s" % (readable, filename)

        done_filename = filename + ".done"
        busy_filename = filename + ".busy"

        # prevent the race condition where many threads want to
        # start writing a new cache file at the same time
        time.sleep(random.uniform(0, 0.5))

        # if the result is cahced or being calculated elsewhere,
        if os.access(done_filename, os.F_OK) or \
           os.access(busy_filename, os.F_OK):
            printed = False
            # wait for the other calculation process to finish
            while(not os.access(done_filename, os.F_OK)):
                time.sleep(1.)
                if not printed:
                    print "waiting for %s" % filename
                    printed = True

            # if many threads were waiting to read, space their reads
            time.sleep(random.uniform(0, 0.5))

            print "ready to read %s" % filename
            try:
                input_shelve = shelve.open(filename, "r", protocol=-1)
                retval = input_shelve['result']
                input_shelve.close()
                print "used cached value %s" % filename
            except:
                raise ValueError
        else:
            # first flag the cachefile as busy so other threads wait
            busyfile = open(busy_filename, "w")
            busyfile.write("working")
            busyfile.close()

            # recalculate the function
            print "no cache, recalculating %s" % filename
            start = time.time()
            retval = func(*args, **kwargs)

            outfile = shelve.open(filename, "n", protocol=-1)
            outfile["signature"] = identifier
            outfile["filename"] = filename
            outfile["funcname"] = funcname
            outfile["args"] = rehashed
            outfile["kwargs"] = kwargs
            outfile["result"] = retval
            outfile.close()
            time.sleep(0.2)

            # indicate that the function is done being recalculated
            donefile = open(done_filename, "w")
            donefile.write("%10.15f\n" % (time.time() - start))
            donefile.close()

            # remove the busy flag
            os.remove(busy_filename)

        return retval

    return functools.update_wrapper(memoize, func)


@memoize_persistent
def fibonacci(n):
    """Example: return the nth fibonacci number."""
    if n in (0, 1):
        return n

    return fibonacci(n-1) + fibonacci(n-2)


@memoize_persistent
def useless_loop(input_var, arg1='a', arg2=2):
    r"""Useless function to test the memoize decorator"""
    return (input_var, arg1, arg2)


if __name__ == "__main__":
    print fibonacci(5)
    print fibonacci(5)
    print useless_loop(10, arg1=3, arg2='b')
    print useless_loop(10, arg2='b', arg1=4)
    print useless_loop("w", arg2="ok")
    print useless_loop(10)
    print useless_loop("w")

