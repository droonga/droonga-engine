DROONGA_BASE_DIR = $(HOME)/droonga

# setups droonga-engine
install:
	sudo apt-get update
	sudo apt-get -y upgrade
	sudo apt-get install -y ruby ruby-dev build-essential
	sudo gem install droonga-engine
	mkdir $(DROONGA_BASE_DIR)
	cd $(DROONGA_BASE_DIR)
	droonga-engine-catalog-generate --output=./catalog.json
	echo "host: " > droonga-engine.yaml

