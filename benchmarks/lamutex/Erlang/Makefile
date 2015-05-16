
all: lamutex.beam

lamutex.beam: lamutex.erl
	erlc lamutex.erl

run: lamutex.beam
	erl -noshell -run lamutex start 10 5

clean:
	rm *.beam

.PHONY: all run run2 clean
