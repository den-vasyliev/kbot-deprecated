.PHONY: all build unit-test clean

all: build 
test: unit-test

PLATFORM=arm

BUILDER=docker

build:
	@echo "Let's build it"
	${BUILDER} build . --no-cache --build-arg APP_BUILD_INFO=${VERSION}

unit-test:
	@echo "Run tests here..."
	@${BUILDER} build --target unit-test .

lint:
	@echo "Run lint here..."
	@${BUILDER} build --target lint .

clean:
	@echo "Cleaning up..."
	rm -rf bin