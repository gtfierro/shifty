from graph import DATA, SHAPES

import shifty
from shifty.experimental import vio

case = vio.Universe(DATA, SHAPES).draw(2, seed=0)

# ---
broken = case.graph()
conforms, _, text = shifty.validate(broken, SHAPES)
print(text)
