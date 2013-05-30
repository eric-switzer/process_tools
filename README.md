process_tools
=============

Tools for persistence, trivial parallel jobs. Under development.

* `memoize`: standard in-memory memoize decorator implementation
* `persistent_memoize`: memoize decorator that caches results to a file for later use
* `memoize_batch`: calls a function with several arguments to cache results to a file. These are used in subsequent calls.
* `aggregate_outputs`: coordinated delayed execution with multiprocessing, combine results
* `persistent_class`: pickle classes, or subsets of persistent class variables
