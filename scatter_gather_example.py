from process_tools import scatter_gather as sg
import time

def example_function(arg1, arg2, kwarg=None):
    time.sleep(2)
    print "running", arg1, arg2, kwarg
    return arg1 + arg2, kwarg

if __name__ == "__main__":
    test_sg = sg.ScatterGather("scatter_gather_example.example_function")
    test_sg.scatter("a1", "a2")
    test_sg.scatter("a1", "a2", execute_key="one")
    test_sg.scatter("a3", "a4", kwarg="ok", execute_key="two")
    test_sg.scatter("a5", "a6", kwarg="no", execute_key="three")
    test_sg.gather()
