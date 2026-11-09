"""Write every Guideline 36 ``.shm`` file from the Python modules here.

    uv run --project python python examples/shapeshift/g36/generate.py [out-dir]

With no directory, it rewrites the files next to this script.
``python/tests/test_shapeshift.py`` checks that they are up to date.
"""

from __future__ import annotations

import pathlib
import sys

from afdd import dual_fan, fcu, mz, sz
from air_handlers import mz_ahu, sz_ahu
from terminal_units import discharge, fan_powered, inlet, reheat, vav

from shifty.experimental import shapeshift as ss

MODULES = [
    vav,
    reheat,
    fan_powered,
    inlet,
    discharge,
    mz_ahu,
    sz_ahu,
    mz,
    dual_fan,
    sz,
    fcu,
]

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).parent
    for written in ss.write(out, *MODULES):
        print(f"wrote {written}")
