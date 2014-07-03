from process_tools import scatter_gather as sg
import time
import numpy as np

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
