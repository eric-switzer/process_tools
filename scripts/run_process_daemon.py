#!/usr/bin/python
from process_tools import process_daemon as pd
from optparse import OptionParser
import os
import sys


if __name__ == '__main__':
    r"""main command-line interface"""

    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")

    parser.add_option("-k", "--killfile",
                      action="store",
                      dest="killfile",
                      default="kill",
                      help="If this file exists, shutdown",)

    parser.add_option("-n", "--n_worker",
                      action="store",
                      dest="n_worker",
                      default=5,
                      help="Number of workers to spawn",)

    (options, args) = parser.parse_args()
    optdict = vars(options)

    if len(args) != 0:
        parser.error("wrong number of arguments")

    sys.path.append(os.getcwd())
    pd.start_workers(optdict)


