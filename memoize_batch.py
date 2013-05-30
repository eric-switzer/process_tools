import shelve
import utils
import multiprocessing
import copy
import cPickle as pickle
import hashlib
import re


def _function_wrapper(args_package):
    """Allow multiprocessing Pool to call generic functions/args/kwargs.

    Data are saved here rather than handed back to avoid the scenario where
    all of the output from a batch run is held in memory.
    """
    (identifier, directory, funcname, args, kwargs) = args_package

    readable = utils.readable_call(funcname, args, kwargs)
    filename = "%s/%s.shelve" % (directory, identifier)
    filename = re.sub('/+', '/', filename)
    print "%s -> %s" % (readable, filename)

    result = utils.func_exec(funcname, args, kwargs, printcall=False)

    outfile = shelve.open(filename, 'n', protocol=-1)
    outfile["identifier"] = identifier
    outfile["filename"] = filename
    outfile["funcname"] = funcname
    outfile["args"] = args
    outfile["kwargs"] = kwargs
    outfile["result"] = result
    outfile.close()

    return identifier


class MemoizeBatch(object):
    r"""Manage/cache a large batch of function calls

    Use this when you have a large set of function evaluations you would like
    to perform in parallel an use later from a cache.

    To specify the batch of parameters to run over, initialize the class with
    `generate=True`. Then when execute() is called, it stacks up the list of
    parameters in the call in `self.call_stack`. To perform the computation,
    call one of the call_stack handlers (either multiprocess_stack or
    pbsprocess_stack) to parallelize the function call.

    To use the cached values, initialize the class with `generate=False`
    (default) and then call execute in the same way as above. The cached
    results will be returned by looking up the hash for the arguments.

    Notes:
    The output from each function call must be serializable. It is saved to a
    file with a unique SHA224 identifier based on its function name, args, and
    kwargs.

    For a function func(arg=True), the identifiers for func(arg=True) and
    func() will be different even if their outputs are the same.

    Dictionaries should not be given as arguments. They are not sorted here and
    may result in different identifiers for the same arguments. If you are
    arguments that are json-serializable, consider adding Simplejson's
    sort_keys=True to allow dictionary arguments. This will not work for numpy.

    Example to generate the cache table:
    >>> caller1 = MemoizeBatch("memoize_batch.trial_function", "./", generate=True)
    >>> caller1.execute("ok", 3, arg1=True, arg2=3)
    '12412d7d81bd3bd6c1a86d93ee2ca06f0af35bdfa2a37e86e762557e'
    >>> import numpy as np
    >>> bins = np.arange(0,1, 0.2)
    >>> caller1.execute(5, "ok2", arg1="stringy", arg2=bins)
    'a4bcca2aa43785944dfaf4ef692931e274956ab11ee7b48c6cbf6a59'
    >>> caller1.execute("ok2",4)
    '103a79b6e8a44796ad1b4657c3de31ee38cabc0f1a2810667a297ae4'
    >>> caller1.multiprocess_stack()
    ['12412d7d81bd3bd6c1a86d93ee2ca06f0af35bdfa2a37e86e762557e',
     'a4bcca2aa43785944dfaf4ef692931e274956ab11ee7b48c6cbf6a59',
     '103a79b6e8a44796ad1b4657c3de31ee38cabc0f1a2810667a297ae4']

    Example to use the cache in later computation:
    >>> caller2 = MemoizeBatch("memoize_batch.trial_function", "./")
    >>> caller2.execute("ok",3, arg1=True, arg2=3)
    ('ok', 3, True, 3)
    >>> caller2.execute(5, "ok2", arg1="stringy", arg2=bins)
    (5, 'ok2', 'stringy', array([ 0. ,  0.2,  0.4,  0.6,  0.8]))
    >>> caller2.execute("ok2",4)
    ('ok2', 4, 'e', 'r')

    TODO:
    add regenerate mode?
    assigning run ids to dictionaries of numpy objects: .tolist and json?,
    nested sort + pickle?
    """
    def __init__(self, funcname, directory, generate=False, verbose=False):
        r"""
        funcname: string
            the function name pointer in one of the forms
            [etc].[package].[module].[function_name]

        directory: string
            directory in which to write the output database

        generate: boolean
            choose this to generate the result cache
        """
        self.funcname = funcname
        self.directory = directory
        self.call_stack = []
        self.generate = generate
        self.verbose = verbose

    def execute(self, *args, **kwargs):
        r"""Generate or access data from the function call.

        Note:
        If an "inifile" is one of the function arguments, read that and include
        it in the checksum for the function call identifier.

        also see stackoverflow's: computing-an-md5-hash-of-a-data-structure
        based on: how-to-memoize-kwargs
        """
        kwini = copy.deepcopy(kwargs)
        if "inifile" in kwargs:
            open_inifile = open(kwargs["inifile"], "r")
            kwini["inifile"] = "".join(open_inifile.readlines())
            open_inifile.close()

        argpkl = pickle.dumps((self.funcname, args,
                               tuple(sorted(kwini.items()))), -1)

        identifier = hashlib.sha224(argpkl).hexdigest()

        args_package = (identifier, self.directory,
                        self.funcname, args, kwargs)

        if self.verbose:
            print_call(args_package)

        if self.generate:
            self.call_stack.append(args_package)
            retval = identifier
        else:
            # TODO: raise NotCalculated?
            filename = "%s/%s.shelve" % (self.directory, identifier)
            filename = re.sub('/+', '/', filename)
            input_shelve = shelve.open(filename, "r", protocol=-1)
            retval = input_shelve['result']
            input_shelve.close()

        return retval

    def multiprocess_stack(self, save_cpu=4, debug=False):
        r"""process the call stack built up by 'execute' calls using
        multiprocessing.
        `save_cpu` is the number of CPUs to leave free
        `debug` runs one process at a time because of funny logging/exception
        handling in multiprocessing
        """
        if debug:
            result = []
            for item in self.call_stack:
                print_call(item)
                result.append(_function_wrapper(item))
        else:
            num_cpus = multiprocessing.cpu_count() - save_cpu
            pool = multiprocessing.Pool(processes=num_cpus)
            result = pool.map(_function_wrapper, self.call_stack)
            pool.close()
            pool.join()

        print result

    def pbsprocess_stack(self):
        r"""process the call stack built up by 'execute' call using the PBS
        batch processing protocol
        """
        print "PBS batch not implemented yet"

    def report_cache_table(self):
        r"""read through the cache directory and make a report of the arguments
        of function for which its results are cached.
        """
        print "report cache is not implemented yet"


def trial_function(thing1, thing2, arg1="e", arg2="r"):
    r"""a test function (won't go in docstring"""
    return thing1, thing2, arg1, arg2


if __name__ == "__main__":
    import doctest

    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
