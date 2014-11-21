#!/usr/bin/env python

import sys

__author__ = 'varad'

# Input comes from STDIN (Standard Input)
for line in sys.stdin:

    # Remove leading and trailing whitespaces
    lineHello = line.strip()

    # Split the lines into words
    words = line.split()

    # For each word
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)

"""
[varad@govardhan wordcount]$ echo "foo foo quux labs foo bar quux" | python wcmapper.py
foo	1
foo	1
quux	1
labs	1
foo	1
bar	1
quux	1
"""