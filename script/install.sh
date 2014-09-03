#
# TODO: This script is specific to debian/ubuntu. This should be cross-platform.
#

# install droonga
apt-get update
apt-get -y upgrade
apt-get install -y ruby ruby-dev build-essential
gem install droonga-engine

SCRIPT_URL=https://raw.githubusercontent.com/droonga/droonga-engine/master/script
USER=droonga-engine
DROONGA_BASE_DIR=/home/$USER/droonga

exist_user() {
  grep "^$1:" /etc/passwd > /dev/null
}

# fetch files
curl -O $SCRIPT_URL/droonga-engine -O $SCRIPT_URL/droonga-engine.yaml

# add droonga-engine user and create files
exist_user $USER || useradd -m $USER
droonga-engine-catalog-generate --output=./catalog.json
mkdir $DROONGA_BASE_DIR
mv catalog.json droonga-engine.yaml $DROONGA_BASE_DIR
chown -R $USER.$USER $DROONGA_BASE_DIR

# set up service
mv droonga-engine /etc/init.d/droonga-engine
update-rc.d droonga-engine defaults
