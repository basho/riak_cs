all: build

WWW_DEST = ../../priv/control_ui

clean:
	rm -rf build/* $(WWW_DEST)/*

deps:
	elm-package install -y

dev:
	NODE_ENV=development yarn run start

build:
	NODE_ENV=production yarn run build

install:
	rm -rf $(WWW_DEST)
	mkdir -p $(WWW_DEST)
	cp --archive --recursive --target-directory=$(WWW_DEST) build/*

.PHONY: clean deps build dev
