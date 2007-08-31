#$Id$

now: mime.test simpletest.test

PY ?= python
%.test: %.py
	PYTHONPATH=$(PYTHONPATH):..	$(PY) $< $(ARGS)

#simpletest.test: ARGS=SimpleTest#.testMoveLine
# vim:ts=4:sw=4:noexpandtab

