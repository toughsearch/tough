import os
import sys

sys.path.insert(0, os.path.join(os.path.abspath(".."), "tough"))

pytest_plugins = ["tests.fixtures"]
