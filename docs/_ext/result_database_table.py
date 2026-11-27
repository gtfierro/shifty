"""Print an example SQL query's results as a table for the result database docs.

Documentation tooling, not an example. Run it from an example directory as
``result_database_table.py [script.py ...] query.sql``. It copies the
directory's ``create.py``, the named scripts, and the RDF files to a scratch
directory. It runs ``create.py`` and then each named script there, exactly as
a reader would, and prints the query's rows from the database they wrote.
"""

import os
import runpy
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

*scripts, query_file = sys.argv[1:]
scripts = ["create.py", *scripts]
example = Path.cwd()
query = (example / query_file).read_text()

with tempfile.TemporaryDirectory() as scratch:
    for source in [*(example / script for script in scripts), *example.glob("*.ttl")]:
        shutil.copy(source, scratch)
    os.chdir(scratch)
    for script in scripts:
        runpy.run_path(script)

    connection = sqlite3.connect("building.sqlite")
    cursor = connection.execute(query)
    header = [column[0] for column in cursor.description]
    rows = [
        ["NULL" if value is None else str(value) for value in row] for row in cursor
    ]
    connection.close()
    os.chdir(example)

widths = [max(map(len, column)) for column in zip(header, *rows)]
for line in [header, ["-" * width for width in widths], *rows]:
    print("  ".join(text.ljust(width) for text, width in zip(line, widths)).rstrip())
