# Copyright (C) 2014 Droonga Project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

SCRIPT_URL=https://raw.githubusercontent.com/droonga/droonga-engine/master/script
USER=droonga-engine
DROONGA_BASE_DIR=/home/$USER/droonga

exist_user() {
  grep "^$1:" /etc/passwd > /dev/null
}

setup_configuration_directory() {
  PLATFORM=$1

  [ ! -e $DROONGA_BASE_DIR ] &&
    mkdir $DROONGA_BASE_DIR
  [ ! -e $DROONGA_BASE_DIR/catalog.json ] &&
    droonga-engine-catalog-generate --output=$DROONGA_BASE_DIR/catalog.json
  [ ! -e $DROONGA_BASE_DIR/droonga-engine.yaml ] &&
    curl -o $DROONGA_BASE_DIR/droonga-engine.yaml $SCRIPT_URL/$PLATFORM/droonga-engine.yaml
  chown -R $USER.$USER $DROONGA_BASE_DIR
}

install_in_debian() {
  # install droonga
  apt-get update
  apt-get -y upgrade
  apt-get install -y ruby ruby-dev build-essential
  gem install droonga-engine --no-rdoc --no-ri

  # add droonga-engine user and create files
  exist_user $USER || useradd -m $USER

  setup_configuration_directory debian

  # set up service
  [ ! -e /etc/init.d/droonga-engine ] &&
    curl -o /etc/init.d/droonga-engine $SCRIPT_URL/debian/droonga-engine
  update-rc.d droonga-engine defaults
}

install_in_centos() {
  yum update
  yum -y groupinstall development
  yum -y install ruby-devel
  gem install droonga-engine --no-rdoc --no-ri

  # add droonga-engine user and create files
  exist_user $USER || useradd -m $USER

  setup_configuration_directory centos

  [ ! -e /etc/rc.d/init.d/droonga-engine ] &&
    curl -o /etc/rc.d/init.d/droonga-engine $SCRIPT_URL/centos/droonga-engine
  /sbin/chkconfig --add droonga-engine
}

if [ -e /etc/debian_version ] || [ -e /etc/debian_release ]; then
  install_in_debian
elif [ -e /etc/centos-release ]; then
  install_in_centos
else
  echo "Not supported platform. This script works only for Debian or CentOS."
  return 255
fi
