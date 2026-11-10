from graph import DATA, SHAPES

import shifty
from shifty.experimental import vio

case = vio.Universe(DATA, SHAPES).draw(2, seed=0)

# ---
repaired = case.apply(case.repair)
conforms, _, text = shifty.validate(repaired, SHAPES)
print(conforms)
