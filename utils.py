import time
import logging as log


def log_timing_func():
    '''Decorator generator that logs the time it takes a function to execute'''
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
    '''Decorator generator that logs the time it takes a function to execute'''
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


def short_repr(input_var, maxlen=None):
    r"""return a repr() for something if it is short enough
    could also use len(pickle.dumps()) to see if the object and not just its
    repr is too big to bother printing.
    """
    repr_out = repr(input_var)
    if maxlen:
        if len(rep_rout) > maxlen:
            repr_out = "BIG_ARG"

    return repr_out


# TODO: do not print long arguments
def print_call(args_package):
    r"""Print the function and its arguments and the file in which the outputs
    are saved in the cache directory."""
    (signature, directory, funcname, args, kwargs) = args_package

    kwlist = ["%s=%s" % (item, short_repr(kwargs[item])) for item in kwargs]
    kwstring = ", ".join(kwlist)
    argstring = ", ".join([short_repr(item) for item in args])

    filename = "%s/%s.shelve" % (directory, signature)
    filename = re.sub('/+', '/', filename)

    print "%s(%s, %s) -> %s" % (funcname, argstring, kwstring, filename)

    return filename


def function_wrapper(args_package):
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



