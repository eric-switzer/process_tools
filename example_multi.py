import aggregate_outputs
# TODO: hdf5 outputs


def test_batch(funcname="aggregate_outputs.example_function",
               fpath="input.hdf5", kwarg=None):
    test_agg = aggregate_outputs.AggregateOutputs(funcname)

    # define variables that are common between all the runs
    arg1 = "a1"

    for arg2 in ["b1", "b2", "b3"]:
        execute_key = "%s/%s" % (arg1, arg2)
        test_agg.execute(fpath + arg1, arg2, kwarg=kwarg,
                         execute_key=execute_key)

    calc_product = test_agg.multiprocess_stack()
    print calc_product


if __name__ == "__main__":
    test_batch(fpath="input1.hdf5")
    test_batch(fpath="input2.hdf5", kwarg=True)
