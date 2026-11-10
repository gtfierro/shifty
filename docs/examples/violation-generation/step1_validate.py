from graph import DATA, SHAPES

# ---
import shifty

conforms, _, text = shifty.validate(DATA, SHAPES)
print(conforms)
