#$Id$

import unittest
import sys

suite = unittest.TestSuite()
verbosity = sys.argv.count( '-v')
tests = [a for a in sys.argv[1:] if a != '-v']
if not tests:
    tests = 'convertertest simpletest conditiontest guesstest'.split()

for test in tests:
    suite.addTest( unittest.TestLoader().loadTestsFromName( test))
unittest.TextTestRunner( verbosity= 1+verbosity).run( suite)

# vim:ts=4:sw=4:expandtab
