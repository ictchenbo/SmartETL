import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
ROOT = 'smartetl'
LOADER_MODULE = "loader"
PROCESSOR_MODULE = "iterator"
UTIL_MODULE = "util"


def relative_path(path):
    return os.path.join(PROJECT_ROOT, path)
