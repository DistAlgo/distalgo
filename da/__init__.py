# runtime package

from da import common, api, pattern as pat, compiler, sim
from da.sim import DistProcess, NodeProcess
from da.__main__ import libmain, __version__

import_da = api.import_da
__all__ = ["__version__", "pat", "api", "libmain", "compiler",
           "DistProcess", "NodeProcess",
           "import_da"]

for name in common.api_registry.keys():
    globals()[name] = common.api_registry[name]
    __all__.append(name)
