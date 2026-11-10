from graph import DATA, SHAPES

# ---
from shifty.experimental import vio

universe = vio.Universe(DATA, SHAPES)
print(len(universe), "ways to break the graph")

case = universe.draw(2, seed=0)
for injection in case.injections:
    print(injection.focus, injection.operations[0].detail)
