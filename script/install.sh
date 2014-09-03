#
# TODO: This script is specific to debian/ubuntu. This should be cross-platform.
#

# install droonga
apt-get update
apt-get -y upgrade
apt-get install -y ruby ruby-dev build-essential
gem install droonga-engine

exist_user() {
  grep "^$1:" /etc/passwd > /dev/null
}

# fetch files
SCRIPT_URL=https://raw.githubusercontent.com/droonga/droonga-engine/master/script
curl -O $SCRIPT_URL/droonga-engine -O $SCRIPT_URL/droonga-engine.yaml

# add droonga-engine user and create files
USER=droonga-engine
exist_user $USER || useradd -m $USER

DROONGA_BASE_DIR=/home/$USER/droonga
droonga-engine-catalog-generate --output=./catalog.json
mkdir $DROONGA_BASE_DIR
mv catalog.json droonga-engine.yaml $DROONGA_BASE_DIR
chown -R $USER.$USER $DROONGA_BASE_DIR

# set up service
mv droonga-engine /etc/init.d/droonga-engine
update-rc.d droonga-engine defaults
