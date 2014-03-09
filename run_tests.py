#!/usr/bin/env python
# This scripts runs all tests for HyperDexGraph

import os
import glob

print('Running all modules in tests/')


for file in glob.glob("tests/*.py"):
    os.system('nosetests -v ' + file)
