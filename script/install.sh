#
# TODO: This script is specific to debian/ubuntu. This should be cross-platform.
#

# install droonga
apt-get update
apt-get -y upgrade
apt-get install -y ruby ruby-dev build-essential
gem install droonga-engine --no-rdoc --no-ri

SCRIPT_URL=https://raw.githubusercontent.com/droonga/droonga-engine/master/script/debian
USER=droonga-engine
DROONGA_BASE_DIR=/home/$USER/droonga

exist_user() {
  grep "^$1:" /etc/passwd > /dev/null
}

# add droonga-engine user and create files
exist_user $USER || useradd -m $USER

[ ! -e $DROONGA_BASE_DIR ] &&
  mkdir $DROONGA_BASE_DIR
[ ! -e $DROONGA_BASE_DIR/catalog.json ] &&
  droonga-engine-catalog-generate --output=$DROONGA_BASE_DIR/catalog.json
[ ! -e $DROONGA_BASE_DIR/droonga-engine.yaml ] &&
  curl -o $DROONGA_BASE_DIR/droonga-engine.yaml $SCRIPT_URL/droonga-engine.yaml
chown -R $USER.$USER $DROONGA_BASE_DIR

# set up service
[ ! -e /etc/init.d/droonga-engine ] &&
  curl -o /etc/init.d/droonga-engine $SCRIPT_URL/droonga-engine
update-rc.d droonga-engine defaults
