#$Id$

now: tests/convertertest.test tests/simpletest.test tests/guesstest.test tests/conditiontest.test

PY ?= python
%.test: %.py
	PYTHONPATH=..:../../..:$(PYTHONPATH)	$(PY) $< $(ARGS)

#simpletest.test: ARGS=SimpleTest#.testMoveLine

# vim:ts=4:sw=4:noexpandtab

