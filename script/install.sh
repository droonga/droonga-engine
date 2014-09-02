# install droonga
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install -y ruby ruby-dev build-essential
sudo gem install droonga-engine

# fetch files
#SCRIPT_URL=https://raw.github.com/droonga/droonga-engine/script
#curl -O $SCRIPT_URL/droonga-engine -O $SCRIPT_URL/droonga-engine.yaml

# add droonga-engine user and create files
USER=droonga-engine
sudo useradd -m $USER

login droonga-engine
DROONGA_BASE_DIR=/home/droonga-engine/droonga
mkdir $DROONGA_BASE_DIR
cd $DROONGA_BASE_DIR
droonga-engine-catalog-generate --output=./catalog.json
echo "host: " > droonga-engine.yaml
exit

# set up service
sudo cp droonga-engine /etc/init.d/droonga-engine
sudo update-rc.d droonga-engine defaults
