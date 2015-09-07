
MOCHA_OPTS= --check-leaks
REPORTER = spec

test: build test-unit test-integration

build:
	./node_modules/.bin/babel lib --out-dir dist

test-unit:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--compilers js:mocha-traceur \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS) \
		test/unit/**

test-integration:
	@NODE_ENV=test node test/integration/runner.js

test-load:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS) \
		test/load/**
