# install droonga
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install -y ruby ruby-dev build-essential
sudo gem install droonga-engine

# add droonga-engine user and create files
adduser droonga-engine

login droonga-engine
DROONGA_BASE_DIR = /home/droonga-engine/droonga
mkdir $DROONGA_BASE_DIR
cd $DROONGA_BASE_DIR
droonga-engine-catalog-generate --output=./catalog.json
echo "host: " > droonga-engine.yaml
exit
