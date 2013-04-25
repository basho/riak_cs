.PHONY: test all clean
.DEFAULT: all

all: test

phpunit:
	@composer install --dev

vendor composer.lock: composer.json
	@composer install --dev

test: phpunit
	@./phpunit

clean:
	@rm -rf composer.lock vendor phpunit
