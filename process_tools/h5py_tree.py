"""
Utilities to read/write/print a dictionary tree of array data to an hdf5 file
"""
import numpy as np
import h5py
import os
import tree_tools as tt


def _traverse_data_dict(data_dict, h5pyobj, path=()):
    """TODO: pass only subgroup instead of whole hdf5 object?
    """
    for data_key in data_dict:
        current_path = path+(data_key,)
        if isinstance(data_dict[data_key], np.ndarray):
            #print "npytree -> hdf5", current_path, data_dict[data_key].shape
            pathstr = "/".join(current_path)
            h5pyobj[pathstr] = data_dict[data_key]
        elif not isinstance(data_dict[data_key], dict):
            pass
        else:
            #sub_h5pyobj = h5pyobj.create_group(data_key)
            #_traverse_data_dict(data_dict[data_key],
            #                                 sub_h5pyobj, current_path)
            _traverse_data_dict(data_dict[data_key],
                                             h5pyobj, current_path)


def _print_hd5_tree(data_dict, depth=""):
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
                _print_hd5_tree(data_dict[data_key], current_depth)


def convert_numpytree_hdf5(data_dict, filename, path="", silent=True):
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
    >>> convert_numpytree_hdf5(tdict, "test.hd5", path="path/to", silent=False)
    writing hd5file test.hd5 from dict tree
     -path:
      -to:
       ->data_a (3, 3)
       -dir_a:
        ->data_b (4, 4)
        -dir_c:
         ->data_c (5, 5)
    >>> os.remove("test.hd5")
    """
    outfile = h5py.File(filename, "a")

    if not silent:
        print "writing hd5file %s from dict tree" % filename

    path = tuple(path.split("/"))
    _traverse_data_dict(data_dict, outfile, path=path)
    if not silent:
        _print_hd5_tree(outfile)

    outfile.close()


def _traverse_hd5_dict(h5pyobj):
    outdict = {}
    for data_key in h5pyobj:
        try:
            data_here = h5pyobj[data_key].value
            outdict[data_key] = data_here
        except AttributeError:
            outdict[data_key] = _traverse_hd5_dict(h5pyobj[data_key])

    return outdict


def _print_dict_tree(data_dict, depth=""):
    for data_key in data_dict:
        current_depth = depth + " "
        data_here = data_dict[data_key]
        if type(data_here) is dict:
            print "%s-%s:" % (current_depth, data_key)
            _print_dict_tree(data_here, current_depth)
        else:
            if isinstance(data_here, np.ndarray):
                datashape = repr(data_here.shape)
                print "%s->%s %s" % (current_depth, data_key, datashape)
            else:
                print "%s->%s %s" % (current_depth, data_key, data_here)


def convert_hdf5_dict_tree(h5pyobj, path=(), silent=True):
    r"""
    pull a sub-tree out of an hdf5 file and put into dictionary tree

    >>> tdict = {}
    >>> tdict['dir_a'] = {}
    >>> tdict['data_a'] = np.zeros((3,3))
    >>> tdict['dir_a']['data_b'] = np.zeros((4,4))
    >>> convert_numpytree_hdf5(tdict, "test.hd5", path="path/to", silent=False)
    writing hd5file test.hd5 from dict tree
     -path:
      -to:
       ->data_a (3, 3)
       -dir_a:
        ->data_b (4, 4)
    >>> h5pyobj = h5py.File("test.hd5", "r")
    >>> data = convert_hdf5_dict_tree(h5pyobj['path/to'], silent=False)
     ->data_a (3, 3)
     -dir_a:
      ->data_b (4, 4)
    >>> h5pyobj.close()
    >>> os.remove("test.hd5")
    """
    outdict = _traverse_hd5_dict(h5pyobj)

    if not silent:
        _print_dict_tree(outdict)

    return outdict


def aggregate_hdf5(h5pyobj):
    """Aggregate the resultant data structures from each job
    TODO: add a wildcard so that not all keys are used
    """
    agg_list = []
    for entry_name in h5pyobj.keys():
        data_tree = convert_hdf5_dict_tree(h5pyobj[entry_name])
        agg_list.append(data_tree)

    return tt.aggregate_tree(agg_list)


if __name__ == "__main__":
    import doctest

    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
