# install droonga
apt-get update
apt-get -y upgrade
apt-get install -y ruby ruby-dev build-essential
gem install droonga-engine

# fetch files
#SCRIPT_URL=https://raw.github.com/droonga/droonga-engine/script
#curl -O $SCRIPT_URL/droonga-engine -O $SCRIPT_URL/droonga-engine.yaml

# add droonga-engine user and create files
USER=droonga-engine
useradd -m $USER

DROONGA_BASE_DIR=/home/$USER/droonga
droonga-engine-catalog-generate --output=./catalog.json
mkdir $DROONGA_BASE_DIR
mv catalog.json droonga-engine.yaml $DROONGA_BASE_DIR
chown -R $USER.$USER $DROONGA_BASE_DIR

# set up service
mv droonga-engine /etc/init.d/droonga-engine
update-rc.d droonga-engine defaults
