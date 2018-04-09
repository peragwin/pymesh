
FLASH = ampy

SRCS = $(wildcard *.py)
SRCS += $(wildcard message/*.py)
SRCS += $(wildcard node/*.py)
SRCS += $(wildcard store/*.py)
SRCS += $(wildcard routing/*.py)

flash:
	for s in $(SRCS); do \
		echo flash $$s; \
		./flash $(DEV_PORT) $$s; \
	done

.PHONY: flash