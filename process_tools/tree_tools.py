import numpy as np

# walking-iterating-over-a-nested-dictionary-of-arbitrary-depth-the-dictionary-re
def walk(d):
    '''
    Walk a tree (nested dicts).

    For each 'path', or dict, in the tree, returns a 3-tuple containing:
    (path, sub-dicts, values)

    where:
    * path is the path to the dict
    * sub-dicts is a tuple of (key,dict) pairs for each sub-dict in this dict
    * values is a tuple of (key,value) pairs for each (non-dict) item in this
    * dict
    '''
    # nested dict keys
    nested_keys = tuple(k for k in d.keys() if isinstance(d[k],dict))
    # key/value pairs for non-dicts
    items = tuple((k,d[k]) for k in d.keys() if k not in nested_keys)

    # return path, key/sub-dict pairs, and key/value pairs
    yield ('/', [(k,d[k]) for k in nested_keys], items)

    # recurse each subdict
    for k in nested_keys:
        for res in walk(d[k]):
            # for each result, stick key in path and pass on
            res = ('/%s' % k + res[0], res[1], res[2])
            yield res


# SO: how-to-parse-a-directory-structure-into-dictionary
def set_leaf(tree, branches, leaf):
    """ Set a terminal element to *leaf* within nested dictionaries.
    *branches* defines the path through dictionnaries.

    >>> t = {}
    >>> set_leaf(t, ['b1','b2','b3'], 'new_leaf')
    >>> print t
    {'b1': {'b2': {'b3': 'new_leaf'}}}
    """
    if len(branches) == 1:
        tree[branches[0]] = leaf
        return
    if not tree.has_key(branches[0]):
        tree[branches[0]] = {}
    set_leaf(tree[branches[0]], branches[1:], leaf)


def flatten_dict(dict_in):
    """walks through a nested dictionary tree of numpy arrays
    breaks into directories of data

    >>> tdict = {}
    >>> tdict['dir_a'] = {}
    >>> tdict['data_a'] = np.zeros((1,1))
    >>> tdict['dir_b'] = {}
    >>> tdict['dir_a']['dir_c'] = {}
    >>> tdict['dir_a']['data_b'] = np.zeros((1,1))
    >>> tdict['dir_a']['dir_c']['data_c'] = np.zeros((1,1))
    >>> tdict['dir_b']['cats'] = "this"
    >>> tdict['dir_b']['dogs'] = [1,2,3,4]
    >>> flatten_dict(tdict)
    {'/data_a': array([[ 0.]]), '/dir_a/data_b': array([[ 0.]]),
    '/dir_a/dir_c/data_c': array([[ 0.]])}
    """
    flat_dict = {}
    for (path, dicts, items) in walk(dict_in):
        for key,val in items:
            if isinstance(val, np.ndarray):
                flat_dict[path + key] = val

    return flat_dict


def unflatten_dict(flat_dict):
    """undoes flattening operatio

    >>> tdict = {}
    >>> tdict["/data_a"] = np.array([[ 0.]])
    >>> tdict["/dir_a/data_b"] = np.array([[ 0.]])
    >>> tdict["/dir_a/dir_b/data_c"] = np.array([[ 0.]])
    >>> unflatten_dict(tdict)
    {'data_a': array([[ 0.]]), 'dir_a': {'data_b': array([[ 0.]]), 'dir_b':
    {'data_c': array([[ 0.]])}}}
    """
    ret_dict = {}
    for k, v in flat_dict.iteritems():
        pathsplit = k.split("/")
        directory = pathsplit[1:]
        set_leaf(ret_dict, directory, v)

    return ret_dict


def aggregate_tree(tree_list):
    """take a list of nested dictionaries and combine all their data
    this is useful for parallel jobs, where each job produces a tree of data
    you then want a structure that has the output of all parallel jobs
    np.mean and np.stdev can run on this structure (as arrays)

    >>> tdict = {}
    >>> tdict["/data_a"] = np.ones(shape=(2,3), dtype=float) * 3.141
    >>> tdict["/path/to/data_a"] = np.ones(shape=(2,1), dtype=float) * 2.
    >>> tdict["/some/other/data/here"] = np.ones(shape=(1,3), dtype=int)

    >>> tdict = unflatten_dict(tdict)
    >>> dlist = [ tdict for i in range(10)]

    >>> alist = aggregate_tree(dlist)

    >>> alist = flatten_dict(alist)
    >>> for k,v in alist.iteritems(): print k, v.shape
    /data_a (10, 2, 3)
    /path/to/data_a (10, 2, 1)
    /some/other/data/here (10, 1, 3)
    """
    num_run = len(tree_list)
    flat_list = []
    for item in tree_list:
        flat_list.append(flatten_dict(item))

    agg_tree = {}
    one_entry = flat_list[0]
    for k, v in one_entry.iteritems():
        try:
            dtype = v.dtype
            shape = v.shape
            shape = (num_run,) + shape
            agg_tree[k] = np.zeros(shape, dtype=dtype)
        except:
            print "could not aggregate ", k

    for index, entry in enumerate(flat_list):
        for k, v in entry.iteritems():
            agg_tree[k][index, ...] = v

    unflat_result = unflatten_dict(agg_tree)
    return unflat_result


if __name__ == "__main__":
    import doctest

    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
