from graph import DATA, SHAPES

from shifty.experimental import vio

case = vio.Universe(DATA, SHAPES).draw(2, seed=0)

# ---
for s, p, o in sorted(case.delta.delete):
    print("-", s, p, o)
for s, p, o in sorted(case.delta.add):
    print("+", s, p, o)
