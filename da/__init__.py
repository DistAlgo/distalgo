# runtime package

from . import common, api, pattern as pat, compiler, sim

from .__main__ import libmain, __version__

DistProcess = sim.DistProcess
__all__ = ["__version__", "pat", "api", "libmain", "compiler", "DistProcess"]

for name in common.api_registry.keys():
    globals()[name] = common.api_registry[name]
    __all__.append(name)
