import time
import logging as log


def log_timing_func():
    '''Decorator that logs the time it takes a function to execute
    example for timing your_function():

    from process_tools import utils as proc_util

    @proc_util.log_timing_func
    def your_function():
    '''
    def decorator(func_to_decorate):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func_to_decorate(*args, **kwargs)
            elapsed = (time.time() - start)

            log.debug("[TIMING] %s: %s" % (func_to_decorate.__name__, elapsed))

            return result
        wrapper.__doc__ = func_to_decorate.__doc__
        wrapper.__name__ = func_to_decorate.__name__
        return wrapper
    return decorator


# for classes
def log_timing(func_to_decorate):
    '''Decorator that logs the time it takes a function to execute
    example for timing __init__:

    from process_tools import utils as proc_util

    @proc_util.log_timing
    def __init__(self, ...):
    '''
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func_to_decorate(*args, **kwargs)
        elapsed = (time.time() - start)

        #log.debug("[TIMING] %s: %s" % (func_to_decorate.__name__, elapsed))
        print "[TIMING] %s: %s" % (func_to_decorate.__name__, elapsed)

        return result

    wrapper.__doc__ = func_to_decorate.__doc__
    wrapper.__name__ = func_to_decorate.__name__
    return wrapper


def short_repr(input_var, maxlen=None, repl="BIG_ARG"):
    r"""return a repr() for something if it is short enough

    >>> short_repr(3, maxlen=30)
    '3'
    >>> short_repr("This string is too long", maxlen=5, repl="tl;dr")
    'tl;dr'

    could also use len(pickle.dumps()) to see if the object and not just its
    repr is too big to bother printing.
    """
    repr_out = repr(input_var)
    if maxlen:
        if len(repr_out) > maxlen:
            repr_out = repl

    return repr_out


def readable_call(funcname, args, kwargs, maxlen=None, repl="BIG_ARG"):
    """Print a human-readable expression for a function call: arguments and
    keyword arguments.

    >>> readable_call("test_func", [3,4,5], {'kw1': "this", 'kw2': "that"})
    "test_func(3, 4, 5, kw1='this', kw2='that')"

    >>> readable_call("test_func", [3,4,5], None)
    'test_func(3, 4, 5)'

    >>> readable_call("test_func", None, {'kw1': "this", 'kw2': "that"})
    "test_func(kw1='this', kw2='that')"

    >>> readable_call("test_func", None, None)
    'test_func()'

    Previous ways this was used:
    (signature, directory, funcname, args, kwargs) = args_package
    filename = "%s/%s.shelve" % (directory, signature)
    filename = re.sub('/+', '/', filename)
    print "%s -> %s" % (readable, filename)
    (execute_key, funcname, args, kwargs) = args_package
    print "%s: %s" % (execute_key, readable)
    TODO: deal with spurious commas
    """
    kwlist = []
    try:
        for item in kwargs:
            short_form = short_repr(kwargs[item], maxlen=maxlen, repl=repl)
            kwlist.append("%s=%s" % (item, short_form))
    except TypeError:
        kwlist = []

    arglist = []
    try:
        for item in args:
            short_form = short_repr(item, maxlen=maxlen, repl=repl)
            arglist.append(short_form)
    except TypeError:
        arglist = []

    outstring = "%s(" % funcname
    if arglist:
        outstring += ", ".join(arglist)

    if arglist and kwlist:
        outstring += ", "

    if kwlist:
        outstring += ", ".join(kwlist)

    return outstring + ")"


def function_wrapper(args_package):
    (execute_key, funcname, args, kwargs) = args_package
    print_call(args_package)

    funcattr = None
    funcsplit = funcname.split(".")
    # consider http://docs.python.org/dev/library/importlib.html
    if len(funcsplit) > 1:
        mod = __import__(".".join(funcsplit[0:-1]))
        for comp in funcsplit[1:-1]:
            mod = getattr(mod, comp)

        funcattr = getattr(mod, funcsplit[-1])
    else:
        funcattr = globals()[funcsplit[0]]

    return (args_package, funcattr(*args, **kwargs))


def memoize_function_wrapper(args_package):
    r"""A free-standing function wrapper that supports MemoizeBatch
    (Why? Multiprocessing pool's map can not handle class functions or generic
    function arguments to calls in that pool.)
    Data are saved here rather than handed back to avoid the scenario where
    all of the output from a batch run is held in memory.
    """
    (signature, directory, funcname, args, kwargs) = args_package

    filename = print_call(args_package)

    funcattr = None
    funcsplit = funcname.split(".")

    # consider http://docs.python.org/dev/library/importlib.html
    if len(funcsplit) > 1:
        mod = __import__(".".join(funcsplit[0:-1]))
        for comp in funcsplit[1:-1]:
            mod = getattr(mod, comp)

        funcattr = getattr(mod, funcsplit[-1])
    else:
        funcattr = globals()[funcsplit[0]]

    # some voodoo to prevent lockfile collisions
    time.sleep(random.uniform(0, 2.))
    result = funcattr(*args, **kwargs)

    outfile = shelve.open(filename, 'n', protocol=-1)
    outfile["signature"] = signature
    outfile["filename"] = filename
    outfile["funcname"] = funcname
    outfile["args"] = args
    outfile["kwargs"] = kwargs
    outfile["result"] = result
    outfile.close()

    return signature


if __name__ == "__main__":
    import doctest

    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
