import os

class ClassPersistence(object):
    r"""note that pickle files are convenient but inflexible in that they
    depend on the context of the class instance, so are a pain to open later
    in different contexts -- they are fine to purely save intermediate data.
    """
    def __init__(self, verbose=False):
        if verbose:
            print "Class using ClassPersistence for file IO"

    def save_pickle(self, filename):
        """note that to withold certain attributes (like open file handles)
        from the pickle file to save, use __getstate__ and __setstate__
        as in http://docs.python.org/library/pickle.html#example to delete
        certain items from the class __dict__."""
        print "save_pickle: to file " + filename
        save_pickle(self, filename)

    @classmethod
    def load_pickle(cls, filename):
        r"""reinvigorate a class from a pickle which has saved everything out:
        ok = ClassPersistence.load_pickle(filename)
        """
        print "load_pickle: from file " + filename
        return load_pickle(filename)

    def shelve_variables(self, filename, varlist_in=None):
        r"""save the variables in a class, optionally just those named in
        `varlist_in`. Note that __dict__ of an instance is just user-provided
        variables, but Class.__dict__ here is everything in the class.
        This clobbers anything in the requested file open as 'n'
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
        r"""load variables from a shelve directly into class attributes"""
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
        super(MapPair, self).__init__()
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

