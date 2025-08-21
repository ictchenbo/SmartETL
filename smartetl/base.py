import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
ROOT = 'smartetl'
LOADER_MODULE = "loader"
PROCESSOR_MODULE = "processor"
UTIL_MODULE = "util"
TOP_MODULES = ['loader', 'processor', 'util', 'database', 'gestata']


def relative_path(path):
    return os.path.join(PROJECT_ROOT, path)
