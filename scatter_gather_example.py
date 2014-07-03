from process_tools import scatter_gather as sg
import time
import numpy as np
import h5py
from process_tools import h5py_tree as ht

def example_function(arg1, kwarg=0.):
    time.sleep(2)
    print "running", arg1, kwarg
    rand = np.random.uniform(size=(100,100))
    rerand = rand * arg1 + kwarg
    return {"random": rand, "rescaled": rerand}

if __name__ == "__main__":
    test_sg = sg.ScatterGather("scatter_gather_example.example_function")
    test_sg.scatter(1.)
    test_sg.scatter(2., execute_key="one")
    test_sg.scatter(3., kwarg=1., execute_key="two")
    test_sg.scatter(4., kwarg=2., execute_key="three")
    test_sg.gather("test.hdf5", log_filename="test.log")

    product = h5py.File("test.hdf5", "r")
    compiled = ht.aggregate_hdf5(product)
    product.close()
    for k,v in compiled.iteritems(): print k, v.shape

    ht.convert_numpytree_hdf5(compiled, "test.hdf5", path="compiled")
    # then issue h5ls -r test.hdf5 to see the added data
