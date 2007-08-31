#$Id$

now: mime.test simpletest.test

PY ?= python
%.test: %.py
	PYTHONPATH=$(PYTHONPATH):..	$(PY) $<

# vim:ts=4:sw=4:noexpandtab

