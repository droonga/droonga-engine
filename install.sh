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

# Usage:
#   Install a release version:
#     curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | sudo bash
#   Install the latest revision from the repository:
#     curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | sudo INSTALL_VERSION=master bash

NAME=droonga-engine
SCRIPT_URL=https://raw.githubusercontent.com/droonga/$NAME/master/install
REPOSITORY_URL=https://github.com/droonga/$NAME.git
USER=$NAME
DROONGA_BASE_DIR=/home/$USER/droonga

if [ "$INSTALL_VERSION" = "" ]; then
  export INSTALL_VERSION=release
fi

exist_command() {
  type "$1" > /dev/null 2>&1
}

exist_user() {
  id "$1" > /dev/null 2>&1
}

prepare_user() {
  if ! exist_user $USER; then
    useradd -m $USER
  fi
}

setup_configuration_directory() {
  PLATFORM=$1

  [ ! -e $DROONGA_BASE_DIR ] &&
    mkdir $DROONGA_BASE_DIR
  [ ! -e $DROONGA_BASE_DIR/catalog.json ] &&
    droonga-engine-catalog-generate --output=$DROONGA_BASE_DIR/catalog.json
  [ ! -e $DROONGA_BASE_DIR/$NAME.yaml ] &&
    curl -o $DROONGA_BASE_DIR/$NAME.yaml $SCRIPT_URL/$PLATFORM/$NAME.yaml
  chown -R $USER.$USER $DROONGA_BASE_DIR
}

install_rroonga() {
  # Install Rroonga globally from a public gem, because custom build
  # doesn't work as we expect for Droonga...
  if exist_command grndump; then
    current_version=$(grndump -v | cut -d " " -f 2)
    version_matcher=$(cat droonga-engine.gemspec | \
                      grep rroonga | \
                      cut -d "," -f 2 | \
                      cut -d '"' -f 2)
    compared_version=$(echo "$version_matcher" | \
                       cut -d " " -f 2)
    operator=$(echo "$version_matcher" | cut -d " " -f 1)
    compare_result=$(ruby -e "puts('$current_version' $operator '$compared_version')")
    if [ $compare_result = "true" ]; then return 0; fi
  fi
  gem install rroonga --no-ri --no-rdoc
}

install_master() {
  gem install bundler --no-ri --no-rdoc

  if [ -d $NAME ]
  then
    cd $NAME
    install_rroonga
    git stash save
    git pull --rebase
    git stash pop
    bundle update
  else
    git clone $REPOSITORY_URL
    cd $NAME
    install_rroonga
    bundle install
  fi
  bundle exec rake build
  gem install "pkg/*.gem" --no-ri --no-rdoc
}

install_service_script() {
  INSTALL_LOCATION=$1
  PLATFORM=$2
  DOWNLOAD_URL=$SCRIPT_URL/$PLATFORM/$NAME
  if [ ! -e $INSTALL_LOCATION ]
  then
    curl -o $INSTALL_LOCATION $DOWNLOAD_URL
    chmod +x $INSTALL_LOCATION
  fi
}

install_in_debian() {
  apt-get update
  apt-get -y upgrade
  apt-get install -y ruby ruby-dev build-essential
  if [ "$INSTALL_VERSION" = "master" ]; then
    echo "Installing droonga-engine from the git repository..."
    apt-get install -y git
    install_master
  else
    echo "Installing droonga-engine from RubyGems..."
    gem install droonga-engine --no-rdoc --no-ri
  fi

  prepare_user

  setup_configuration_directory debian

  # register droogna-engine as a service
  install_service_script /etc/init.d/$NAME debian
  update-rc.d $NAME defaults
}

install_in_centos() {
  yum update
  yum -y groupinstall development
  yum -y install ruby-devel git
  install_master

  prepare_user

  setup_configuration_directory centos

  # register droogna-engine as a service
  install_service_script /etc/rc.d/init.d/$NAME centos
  /sbin/chkconfig --add $NAME
}

if [ -e /etc/debian_version ] || [ -e /etc/debian_release ]; then
  install_in_debian
elif [ -e /etc/centos-release ]; then
  install_in_centos
else
  echo "Not supported platform. This script works only for Debian or CentOS."
  return 255
fi
