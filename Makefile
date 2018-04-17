
FLASH = ampy
UPY = micropython

DIRS = agents message node sensors store board

SRCS = $(wildcard *.py)
SRCS += $(wildcard message/*.py)
SRCS += $(wildcard node/*.py)
SRCS += $(wildcard store/*.py)
SRCS += $(wildcard agents/*.py)
SRCS += $(wildcard sensors/*.py)
SRCS += $(wildcard board/*.py)

flash:
	# for dir in $(DIRS); do \
	# 	echo mkdir $$dir; \
	# 	$(FLASH) --port $(DEV_PORT) mkdir $$dir; \
	# done
	for s in $(SRCS); do \
		echo flash $$s; \
		./flash $(DEV_PORT) $$s; \
	done

test:
	$(UPY) -m message.test.main

.PHONY: flash