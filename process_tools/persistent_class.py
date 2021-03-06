import os
import cPickle
import shelve


def save_pickle(pickle_data, filename):
    """Wrap cPickle; useful for general class load/save.

    Notes:
    This clobbers anything in the requested file.
    If you load a pickle outside of the class that saved itself, you
    must fully specify the class space as if you were the class, e.g.:
    from correlate.freq_slices import * to import the freq_slices object
    """
    pickle_out_root = os.path.dirname(filename)
    if not os.path.isdir(pickle_out_root):
        os.mkdir(pickle_out_root)

    pickle_handle = open(filename, 'wb')
    cPickle.dump(pickle_data, pickle_handle, -1)
    pickle_handle.close()


def load_pickle(filename):
    """Return `pickle_data` saved in file with name `filename`."""
    pickle_handle = open(filename, 'r')
    pickle_data = cPickle.load(pickle_handle)
    pickle_handle.close()
    return pickle_data


class ClassPersistence(object):
    """Shelve a class so that you can reload it later.

    One option is to pickle a class instance, but this is inflexible because it
    depends on the context of the class instance. It is difficult to deal with
    that pickle later outside of the context of the class.

    This also gives the option of saving a subset of class data.
    """
    def __init__(self):
        print "Class using ClassPersistence for file IO"

    def save_pickle(self, filename):
        """Pickle the entire class instance.

        TODO: implement  __getstate__ and __setstate__
        see http://docs.python.org/library/pickle.html"""
        print "save_pickle: to file " + filename

        save_pickle(self, filename)

    @classmethod
    def load_pickle(cls, filename):
        """Reinvigorate an entire class from a pickle."""
        print "load_pickle: from file " + filename
        return load_pickle(filename)

    def shelve_variables(self, filename, varlist_in=None):
        """Save a subset of variables in a class.

        `varlist_in` is the list of vars to save
        if this is not given, use self.varlist

        Notes:
        This clobbers anything in the requested file; open as 'n'
        __dict__ of an instance is just user-provided variables
        Class.__dict__ here is everything in the class
        """
        shelve_out_root = os.path.dirname(filename)

        if not os.path.isdir(shelve_out_root):
            os.mkdir(shelve_out_root)

        shelveobj = shelve.open(filename, 'n')

        message = "shelve_variables: "
        if varlist_in is None:
            try:
                varlist_in = self.varlist
                message += "varlist specified in-class: "
            except:
                varlist_in = self.__dict__.keys()
                message += "saving all attributes: "
                #shelveobj.update(self.__dict__)
        else:
            message += "saving to varlist: "

        message += "%s to %s" % (varlist_in, filename)

        print message

        for key in varlist_in:
            shelveobj[key] = self.__dict__[key]

        shelveobj.close()

    def load_variables(self, filename, varlist_in=None):
        """load variables from a shelve directly into class attributes

        `varlist_in` is the list of vars to save
        if this is not given, use self.varlist
        """
        shelveobj = shelve.open(filename, 'r')

        message = "load_variables: "
        if varlist_in is None:
            try:
                varlist_in = self.varlist
                message += "varlist specified in-class: "
            except:
                varlist_in = shelveobj.keys()
                message += "loading all attributes: "
        else:
            message += "saving to varlist: "

        message += "%s from %s" % (varlist_in, filename)

        print message

        for key in varlist_in:
            try:
                setattr(self, key, shelveobj[key])
            except KeyError:
                print "ERROR: requested variable %s not in shelve!" % key

        shelveobj.close()


class TestClassPersistence(ClassPersistence):
    r"""Example class for ClassPersistence object

    One option the load_variables provides is to split the __init__ into to two
    cases; 1) usual initialization with all variables handed in, 2) a shelve
    file initialization where some key variables are refreshed from the shelve
    file.

    # test the full recovery via pickle files
    >>> test = TestClassPersistence('test', [[0,0],[1,1]])
    Class using ClassPersistence for file IO
    >>> print "original class __dict__:" + repr(test.__dict__)
    original class __dict__:{'varlist': ['var1', 'var2'],
                             'var1': 'test', 'var2': [[0, 0], [1, 1]]}
    >>> pklfile = "/tmp/testClassPersistence.pkl"
    >>> shelvefile = "/tmp/testClassPersistence.shelve"

    # test the pickle
    >>> test.save_pickle(pklfile)
    save_pickle: to file /tmp/testClassPersistence.pkl
    >>> test2 = TestClassPersistence.load_pickle(pklfile)
    load_pickle: from file /tmp/testClassPersistence.pkl
    >>> test2.print_var()
    print_var(): 'test' [[0, 0], [1, 1]]

    # test the shelve
    >>> test.shelve_variables(shelvefile)
    shelve_variables: varlist specified in-class: ['var1', 'var2'] to
        /tmp/testClassPersistence.shelve
    >>> testr = shelve.open(shelvefile)
    >>> print "recovered shelve :" + repr(testr)
    recovered shelve :{'var1': 'test', 'var2': [[0, 0], [1, 1]]}
    >>> testr.close()

    >>> test2 = TestClassPersistence(shelve_filename=shelvefile)
    Class using ClassPersistence for file IO
    load_variables: varlist specified in-class: ['var1', 'var2'] from
        /tmp/testClassPersistence.shelve
    >>> print "shelve-loaded class __dict__:" + repr(test2.__dict__)
    shelve-loaded class __dict__:{'varlist': ['var1', 'var2'],
        'var1': 'test', 'var2': [[0, 0], [1, 1]]}

    # with only one variable
    >>> test.shelve_variables(shelvefile, varlist_in=['var1'])
    shelve_variables: saving to varlist: ['var1'] to
        /tmp/testClassPersistence.shelve
    >>> testr = shelve.open(shelvefile)
    >>> print "recovered shelve: " + repr(testr)
    recovered shelve: {'var1': 'test'}
    >>> testr.close()
    >>> test3 = TestClassPersistence(shelve_filename=shelvefile)
    Class using ClassPersistence for file IO
    load_variables: varlist specified in-class: ['var1', 'var2'] from
        /tmp/testClassPersistence.shelve
    ERROR: requested variable var2 not in shelve!
    >>> print "reduced shelve-loaded class __dict__:" + repr(test3.__dict__)
    reduced shelve-loaded class __dict__:{'varlist': ['var1', 'var2'],
        'var1': 'test'}
    >>> os.remove(pklfile)
    >>> os.remove(shelvefile)
    """
    def __init__(self, *args, **kwargs):
        ClassPersistence.__init__(self)

        # call out variable names to save
        self.varlist = ['var1', 'var2']

        if ((len(args) == 0) and ("shelve_filename" in kwargs)):
            self.shelve_init(*args, **kwargs)
        else:
            self.standard_init(*args, **kwargs)

    def shelve_init(self, *args, **kwargs):
        shelve_filename = kwargs['shelve_filename']
        self.load_variables(shelve_filename)

    def standard_init(self, *args, **kwargs):
        self.var1 = args[0]
        self.var2 = args[1]

    def print_var(self):
        print "print_var(): " + repr(self.var1) + " " + repr(self.var2)


if __name__ == "__main__":
    import doctest

    # run some tests
    OPTIONFLAGS = (doctest.ELLIPSIS |
                   doctest.NORMALIZE_WHITESPACE)
    doctest.testmod(optionflags=OPTIONFLAGS)
