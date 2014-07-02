from process_tools import scatter_gather as sg

test_sg = sg.ScatterGather("scatter_gather.example_function")
test_sg.scatter("a1", "a2")
test_sg.scatter("a1", "a2", execute_key="one")
test_sg.scatter("a3", "a4", kwarg="ok", execute_key="two")
test_sg.scatter("a5", "a6", kwarg="no", execute_key="three")
test_sg.gather()
