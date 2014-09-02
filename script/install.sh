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

DROONGA_BASE_DIR=/home/$USER/droonga
droonga-engine-catalog-generate --output=./catalog.json
sudo mkdir $DROONGA_BASE_DIR
sudo mv catalog.json droonga-engine.yaml $DROONGA_BASE_DIR
sudo chown -R $USER.$USER $DROONGA_BASE_DIR

# set up service
sudo cp droonga-engine /etc/init.d/droonga-engine
sudo update-rc.d droonga-engine defaults
