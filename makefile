#$Id$

PY ?= python
now:
	PYTHONPATH=$(PYTHONPATH):..	$(PY) mime.py

# vim:ts=4:sw=4:noexpandtab

