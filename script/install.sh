SCRIPT_URL=https://raw.githubusercontent.com/droonga/droonga-engine/master/script
USER=droonga-engine
DROONGA_BASE_DIR=/home/$USER/droonga

exist_user() {
  grep "^$1:" /etc/passwd > /dev/null
}

install_in_debian() {
  # install droonga
  apt-get update
  apt-get -y upgrade
  apt-get install -y ruby ruby-dev build-essential
  gem install droonga-engine --no-rdoc --no-ri

  # add droonga-engine user and create files
  exist_user $USER || useradd -m $USER

  [ ! -e $DROONGA_BASE_DIR ] &&
    mkdir $DROONGA_BASE_DIR
  [ ! -e $DROONGA_BASE_DIR/catalog.json ] &&
    droonga-engine-catalog-generate --output=$DROONGA_BASE_DIR/catalog.json
  [ ! -e $DROONGA_BASE_DIR/droonga-engine.yaml ] &&
    curl -o $DROONGA_BASE_DIR/droonga-engine.yaml $SCRIPT_URL/debian/droonga-engine.yaml
  chown -R $USER.$USER $DROONGA_BASE_DIR

  # set up service
  [ ! -e /etc/init.d/droonga-engine ] &&
    curl -o /etc/init.d/droonga-engine $SCRIPT_URL/debian/droonga-engine
  update-rc.d droonga-engine defaults
}

install_in_centos() {
  yum update
  yum -y groupinstall development
  yum -y install ruby-devel npm
  gem install droonga-engine --no-rdoc --no-ri

  # add droonga-engine user and create files
  exist_user $USER || useradd -m $USER

  [ ! -e $DROONGA_BASE_DIR ] &&
    mkdir $DROONGA_BASE_DIR
  [ ! -e $DROONGA_BASE_DIR/catalog.json ] &&
    droonga-engine-catalog-generate --output=$DROONGA_BASE_DIR/catalog.json
  [ ! -e $DROONGA_BASE_DIR/droonga-engine.yaml ] &&
    curl -o $DROONGA_BASE_DIR/droonga-engine.yaml $SCRIPT_URL/centos/droonga-engine.yaml
  chown -R $USER.$USER $DROONGA_BASE_DIR

  [ ! -e /etc/rc.d/init.d/droonga-engine ] &&
    curl -o /etc/rc.d/init.d/droonga-engine $SCRIPT_URL/centos/droonga-engine
  /sbin/chkconfig --add droonga-engine
}

if [ -e /etc/debian_version ] || [ -e /etc/debian_release ]; then
  install_in_debian
elif [ -e /etc/centos-release ]; then
  install_in_centos
else
  echo "This supports only debian, CentOS."
  return 255
fi
