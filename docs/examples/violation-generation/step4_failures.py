from graph import DATA, SHAPES

from shifty.experimental import vio

case = vio.Universe(DATA, SHAPES).draw(2, seed=0)

# ---
for failure in case.failures:
    print(failure.focus, failure.shape)
