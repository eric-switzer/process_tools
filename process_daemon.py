"""
The job file is a shelve that contains
"funcname" function name (including module etc.)
"args" arguments for the function
"kwargs" keyword arguments for the function

the function is run; with outputs:
"retval" returned data of the function
"runlog" captured stdout of the function

to extend this for e.g. PBS, have the running process check for its cpu ID
"""
from multiprocessing import Process, Queue
from optparse import OptionParser
import time
import os
import glob
import sys
import StringIO as StringIO
import utils
import shelve


def _function_wrapper(args_package):
    """helper to wrap function evaluation
    TODO: use multiprocessing logger in case of error?
    multiprocessing.get_logger().error("f%r failed" % (arg_kwargs,))
    """
    (execute_key, funcname, args, kwargs) = args_package
    return (args_package, utils.func_exec(funcname, args, kwargs))


class Worker(Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        #rename job file .running
        #call function through wrapper that handles imports etc.
        #save output a return in job shelve, exit

        while True:
            job_filename = self.queue.get()
            if job_filename == None:
                print "worker received shutdown request"
                return

            print utils.timestamp(), "Starting on: ", job_filename
            # capture stdout to save with the output (otherwise jumbled)
            sys.stdout = StringIO.StringIO()
            utils.proc_info("Process log for job_filename %s" % job_filename)

            try:
                basename = os.path.splitext(job_filename)[0]

                log_filename = "%s.log" % basename
                run_filename = "%s.run" % basename
                done_filename = "%s.done" % basename

                os.rename(job_filename, run_filename)
                jobspec = shelve.open(run_filename, protocol=-1)
                print jobspec
                funcname = jobspec['funcname']
                args = jobspec['args']
                kwargs = jobspec['kwargs']

                retval = utils.func_exec(funcname, args, kwargs)
                jobspec['retval'] = retval
                jobspec.close()
                os.rename(run_filename, done_filename)

                outlog = open(log_filename, "w")
                outlog.write(sys.stdout.getvalue())
                outlog.close()
                sys.stdout = sys.__stdout__
                print utils.timestamp(), "Finished: ", job_filename
            except Exception, e:
                print e


def start_workers(options):
    kill_watchfile = options['killfile']
    n_worker = int(options['n_worker'])

    request_queue = Queue()
    for i in range(n_worker):
        Worker(request_queue).start()

    print "started %d workers" % n_worker

    # check if there are files running; if kill, then drop runner
    while True:
        # if there is a shutdown trigger, apply
        if os.path.isfile(kill_watchfile):
            print "Got signal to kill job handler, exiting."
            for i in range(n_worker):
                request_queue.put(None)

            os.remove(kill_watchfile)
            break

        # else, look for job files
        job_files = glob.glob('*.job')
        if len(job_files) > 0:
            print job_files
            for jfile in job_files:
                request_queue.put(jfile)

        # don't press the filesystem looking for new jobs
        time.sleep(0.1)

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

    start_workers(optdict)
