import multiprocessing
import shelve
import utils
import time
import hashlib
import pickle
import shelve
import process_daemon as pd
"""
multiprocessing scatter gather functions

make sure this will also work in single-threaded mode
"""

def example_function(arg1, arg2, kwarg=None):
    time.sleep(1)
    print "This is a test function", arg1
    return arg1 + arg2, kwarg


def _make_serializable(item):
    """repackage an object so that it can be pickle.dumps()"""
    return item


class ScatterGather(object):
    r"""Spin off a stack of function calls in parallel

    >>> test_sg = ScatterGather("scatter_gather.example_function")
    >>> test_sg.scatter("a1", "a2")
    need to identify an execution key for the task
    >>> test_sg.scatter("a1", "a2", execute_key="one")
    >>> test_sg.scatter("a3", "a4", kwarg="ok", execute_key="two")
    >>> test_sg.scatter("a5", "a6", kwarg="no", execute_key="three")
    """
    def __init__(self, funcname, verbose=False):
        self.call_stack = []
        self.funcname = funcname
        self.verbose = verbose

    def scatter(self, *args, **kwargs):
        if "execute_key" not in kwargs:
            print "need to identify an execution key for the task"
            return

        rehashed = [_make_serializable(item) for item in args]

        argpkl = pickle.dumps((self.funcname, rehashed,
                               tuple(sorted(kwargs.items()))), -1)

        identifier = hashlib.sha224(argpkl).hexdigest()
        readable = utils.readable_call(self.funcname, rehashed, kwargs)

        # delete the kwarg for this function to associate an ID to the output
        # so it does not interfere with the function call.
        execute_key = kwargs["execute_key"]
        del kwargs["execute_key"]

        jobfile_name = "%s/%s.job" % (pd.job_directory, identifier)
        job_shelve = shelve.open(jobfile_name, "n", protocol=-1)
        job_shelve['funcname'] = self.funcname
        job_shelve['args'] = args
        job_shelve['kwargs'] = kwargs
        job_shelve['tag'] = execute_key
        job_shelve['identifier'] = identifier
        job_shelve['call'] = readable

        #print job_shelve
        job_shelve.close()

        self.call_stack.append(identifier)

    def gather(self, outfile):
        # wait for the product files to appear
        # also tack the logs together
        # then delete the scatter files
        # did all the jobs run?
        print outfile


if __name__ == "__main__":
    import doctest

    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
