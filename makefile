#$Id$
#z:BARGS=SimpleTest2
#z:tests/simpletest.test
now: tests/convertertest.test tests/simpletest.test tests/guesstest.test tests/conditiontest.test

PY ?= python
%.test: %.py
	PYTHONPATH=..:../../..:$(PYTHONPATH)	$(PY) $< $(ARGS) -v $(BARGS)

#simpletest.test: ARGS=SimpleTest#.testMoveLine

# vim:ts=4:sw=4:noexpandtab

