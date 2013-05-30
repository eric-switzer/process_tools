import numpy as np
import os
import string
import cPickle
import hashlib
import sys
import time
import shelve
import tempfile
import urllib2
import errno
import h5py
# TODO: use path_properties to check on files other functions here try to write


def _traverse_data_dict(data_dict, h5pyobj, path=()):
    for data_key in data_dict:
        current_path = path+(data_key,)
        if isinstance(data_dict[data_key], np.ndarray):
            #print "npytree -> hdf5", current_path, data_dict[data_key].shape
            h5pyobj[data_key] = data_dict[data_key]
        elif not isinstance(data_dict[data_key], dict):
            pass
        else:
            sub_h5pyobj = h5pyobj.create_group(data_key)
            _traverse_data_dict(data_dict[data_key],
                                             sub_h5pyobj, current_path)


def _print_data_dict(data_dict, depth=""):
    for data_key in data_dict:
        current_depth = depth + " "
        try:
            data_here = data_dict[data_key].value
            if isinstance(data_here, np.ndarray):
                datashape = repr(data_here.shape)
                print "%s->%s %s" % (current_depth, data_key, datashape)
            else:
                print "leaf %s has non numpy data" % data_key
        except AttributeError:
            if isinstance(data_key, (str, unicode)):
                print "%s-%s:" % (current_depth, data_key)
                _print_data_dict(data_dict[data_key], current_depth)


def convert_numpytree_hdf5(data_dict, filename):
    r"""Convert a numpy tree of numpy objects to and hd5 file
    Non-numpy data are not recorded!

    >>> tdict = {}
    >>> tdict['dir_a'] = {}
    >>> tdict['data_a'] = np.zeros((3,3))
    >>> tdict['dir_b'] = {}
    >>> tdict['dir_a']['dir_c'] = {}
    >>> tdict['dir_a']['data_b'] = np.zeros((4,4))
    >>> tdict['dir_a']['dir_c']['data_c'] = np.zeros((5,5))
    >>> tdict['dir_b']['cats'] = "this"
    >>> tdict['dir_b']['dogs'] = [1,2,3,4]
    >>> convert_numpytree_hdf5(tdict, "test.hd5")
    writing hd5file test.hd5 from dict tree
     ->data_a (3, 3)
     -dir_a:
      ->data_b (4, 4)
      -dir_c:
       ->data_c (5, 5)
     -dir_b:
    """
    outfile = h5py.File(filename, "w")
    print "writing hd5file %s from dict tree" % filename
    _traverse_data_dict(data_dict, outfile)
    _print_data_dict(outfile)
    outfile.close()


def print_multicolumn(*args, **kwargs):
    """given a series of arrays of the same size as arguments, write these as
    columns to a file (kwarg "outfile"), each with format string (kwarg format)
    """
    outfile = "multicolumns.dat"
    format = "%10.15g"

    if "outfile" in kwargs:
        outfile = kwargs["outfile"]

    if "format" in kwargs:
        format = kwargs["format"]

    # if there is a root directory that does not yet exist
    rootdir = "/".join(outfile.split("/")[0:-1])
    if len(rootdir) > 0 and rootdir != ".":
        if not os.path.isdir(rootdir):
            print "print_multicolumn: making dir " + rootdir
            os.mkdir(rootdir)

    numarg = len(args)
    fmt_string = (format + " ") * numarg + "\n"
    print "writing %d columns to file %s" % (numarg, outfile)

    outfd = open(outfile, "w")
    for column_data in zip(*args):
        outfd.write(fmt_string % column_data)

    outfd.close()


def mkdir_p(path) :
    """Same functionality as shell command mkdir -p."""
    try:
        os.makedirs(path)
    except OSError, exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else: raise


def mkparents(path) :
    """Given a file name, makes all the parent directories."""
    last_slash = path.rfind('/')
    if last_slash > 0 : # Protect against last_slash == -1 or 0.
        outdir = path[:last_slash]
        mkdir_p(outdir)


def abbreviate_file_path(fname) :
    """Abbrviates file paths.
    Given any file path return an abbreviated path showing only the deepest
    most directory and the file name (Useful for writing feedback that doesn't
    flood your screen.)
    """
    split_fname = fname.split('/')
    if len(split_fname) > 1 :
        fname_abbr = split_fname[-2] + '/' + split_fname[-1]
    else :
        fname_abbr = fname

    return fname_abbr


def load_shelve_over_http(url):
    r"""load a shelve specified by a url into a dictionary as output
    shleve does not accept file handles, so write using tempfile
    there is probably a more elegant way to do this
    """
    req = urllib2.urlopen(url)
    temp_file = tempfile.NamedTemporaryFile()

    chunksize = 16 * 1024
    while True:
        chunk = req.read(chunksize)
        if not chunk: break
        temp_file.write(chunk)
    temp_file.flush()

    shelvedict = shelve.open(temp_file.name, "r")
    retdict = {}
    retdict.update(shelvedict)
    shelvedict.close()
    temp_file.close()

    return retdict


def get_env(name):
    r"""retrieve a selected environment variable"""
    try:
        env_var = os.environ[name]
    except KeyError:
        print "your environment variable %s is not set" % name
        sys.exit()

    return env_var


def repl_bracketed_env(string_in):
    r"""replace any []-enclosed all caps subtring [ENV_VAR] with its
    environment vairable

    >>> repl_bracketed_env("this is your path: [PATH]")
    'this is your path: ...'
    """
    ttable = string.maketrans("[]", "[[")
    breakup = string_in.translate(ttable).split("[")
    retval = []
    for item in breakup:
        if item.isupper():
            item = get_env(item)

        retval.append(item)

    retval = ''.join(retval)

    return retval


def save_pickle(pickle_data, filename):
    r"""wrap cPickle; useful for general class load/save
    note that if you load a pickle outside of the class that saved itself, you
    must fully specify the class space as if you were the class, e.g.:
    from correlate.freq_slices import * to import the freq_slices object
    This clobbers anything in the requested file.
    """
    pickle_out_root = os.path.dirname(filename)
    if not os.path.isdir(pickle_out_root):
        os.mkdir(pickle_out_root)

    pickle_handle = open(filename, 'wb')
    cPickle.dump(pickle_data, pickle_handle, -1)
    pickle_handle.close()


def load_pickle(filename):
    r"""Return `pickle_data` saved in file with name `filename`.
    """
    pickle_handle = open(filename, 'r')
    pickle_data = cPickle.load(pickle_handle)
    pickle_handle.close()
    return pickle_data


def extract_rootdir(pathname):
    r"""superceded by os.path.dirname(filename), but we still use it in
    path_properties because "./" is nice to indicate local paths.
    """
    directory = "/".join(pathname.split("/")[:-1])
    if directory == "":
        directory = "."
    directory += "/"
    return directory


def path_properties(pathname, intend_write=False, intend_read=False,
                    file_index="", prefix="", silent=False, is_file=False,
                    die_overwrite=False):
    r"""Check various properties about the path, print the result
    if `intend_write` then die if the path does not exist or is not writable
    if `intend_read` then die if the path does not exist
    `file_index` is an optional mark to put in front of the file name
    `prefix` is an optional mark to prefix the file property output
    `silent` turns off printing unless there was an intend_r/w error
    `is_file` designates a file; if it does not exist check if the path is
    writable.

    # Usage examples:
    >>> path_properties("this_does_not_exist.txt", is_file=True)
    => this_does_not_exist.txt: does not exist,
        but path: ./ exists, is writable, is not readable.
    (True, False, True, False, '...')

    >>> path_properties("/tmp/nor_does_this.txt", is_file=True)
    => /tmp/nor_does_this.txt: does not exist,
        but path: /tmp/ exists, is writable, is not readable.
    (True, False, True, False, '...')

    # here's one that you should not be able to write to
    >>> path_properties("/var/nor_does_this.txt", is_file=True)
    => /var/nor_does_this.txt: does not exist,
        but path: /var/ exists, is not writable, is not readable.
    (True, False, False, False, '...')

    # here's one that definitely exists
    >>> path_properties(__file__, is_file=True)
    => ...: exists, is writable, is readable.
    (True, True, True, False, '...')

    # test a writeable directory that exists
    >>> path_properties('/tmp/')
    => /tmp/: exists, is writable, is readable.
    (True, True, True, True, '...')
    """
    entry = "%s=> %s %s:" % (prefix, file_index, pathname)

    exists = os.access(pathname, os.F_OK)
    # save the state of the file (exist will later specify if the parent dir
    # exists, but we also care about this specific file)
    file_exists = exists
    readable = os.access(pathname, os.R_OK)
    writable = os.access(pathname, os.W_OK)
    executable = os.access(pathname, os.X_OK)
    if exists:
        modtime = time.ctime(os.path.getmtime(pathname))
    else:
        modtime = None

    # if this is a file that does not exist, check the directory
    if not exists and is_file:
        directory = extract_rootdir(pathname)

        writable = os.access(directory, os.W_OK)
        exists = os.access(directory, os.F_OK)
        if exists:
            modtime = time.ctime(os.path.getmtime(directory))

        readable = False
        entry += " does not exist, but path: %s" % directory

    if exists:
        entry += " exists,"

        if writable:
            entry += " is writable,"
        else:
            entry += " is not writable,"

        if readable:
            entry += " is readable."
        else:
            entry += " is not readable."
    else:
        entry += " does not exist"

    if not silent:
        print entry

    if intend_read and not readable:
        print "ERROR: no file to read: %s" % pathname
        sys.exit()

    if intend_write and not writable:
        print "ERROR: can not write this file"
        sys.exit()

    if intend_write and file_exists:
        if die_overwrite:
            print "OVERWRITE ERROR: " + entry
            sys.exit()
        else:
            print "WARNING: you will overwrite " + entry

    return (exists, readable, writable, executable, modtime)


def hashfile(filename, hasher=hashlib.md5(), blocksize=65536, max_size=1.e12):
    r"""determine the hash of a file in blocks
    if it exceeds `max_size` just report the modification time
    if the file does not exist, report '-1'
    """
    if os.access(filename, os.F_OK):
        if (os.path.getsize(filename) < max_size):
            afile = open(filename, 'r')
            buf = afile.read(blocksize)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(blocksize)

            afile.close()
            hash_digest = hasher.hexdigest()
        else:
            hash_digest = "large_file"

        modtime = time.ctime(os.path.getmtime(filename))
    else:
        hash_digest = "not_exist"
        modtime = "not_exist"

    return (hash_digest, modtime)
