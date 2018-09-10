# runtime package

from . import common
from . import importer
from . import pattern as pat
from .common import global_init
from .sim import DistProcess, NodeProcess

__version__ = VERSION = common.__version__
modules = common.modules

__all__ = ["global_init", "DistProcess", "NodeProcess"]

for name in common.api_registry.keys():
    globals()[name] = common.api_registry[name]
    __all__.append(name)

# Hook into multiprocessing.spawn:
common._install()
