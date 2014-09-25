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
#
#  Ubuntu:
#
#   Install a release version:
#     $ curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | sudo bash
#   Install the latest revision from the repository:
#     $ curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | sudo VERSION=master bash
#   Install with the specified hostname (disabling auto-detection):
#     $ curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | sudo HOST=xxx.xxx.xxx.xxx bash
#
#  CentOS 7:
#
#   Install a release version:
#     # curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | bash
#   Install the latest revision from the repository:
#     # curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | VERSION=master bash
#   Install with the specified hostname (disabling auto-detection):
#     # curl https://raw.githubusercontent.com/droonga/droonga-engine/master/install.sh | HOST=xxx.xxx.xxx.xxx bash

NAME=droonga-engine
SCRIPT_URL=https://raw.githubusercontent.com/droonga/$NAME/master/install
REPOSITORY_URL=https://github.com/droonga/$NAME.git
USER=$NAME
GROUP=droonga
DROONGA_BASE_DIR=/home/$USER/droonga

: ${VERSION:=release}
: ${HOST:=Auto Detect}

REQUIRED_COMMANDS=gem
[ "$VERSION" = "master" ] &&
  REQUIRED_COMMANDS="$REQUIRED_COMMANDS git"

case $(uname) in
  Darwin|*BSD|CYGWIN*) sed="sed -E" ;;
  *)                   sed="sed -r" ;;
esac

exist_command() {
  type "$1" > /dev/null 2>&1
}

exist_all_commands() {
  for command in $@; do
    if ! exist_command $command; then
      return 1
    fi
  done
  return 0
}

exist_user() {
  id "$1" > /dev/null 2>&1
}

prepare_environment() {
  if exist_all_commands $REQUIRED_COMMANDS; then
    return 0
  fi

  echo "Preparing the environment..."
  prepare_environment_in_$PLATFORM
  return 0
}

prepare_user() {
  echo ""
  echo "Preparing the user..."

  groupadd $GROUP

  if ! exist_user $USER; then
    useradd -m $USER
  fi

  usermod -G $GROUP $USER
}

setup_configuration_directory() {
  echo ""
  echo "Setting up the configuration directory..."

  [ ! -e $DROONGA_BASE_DIR ] &&
    mkdir $DROONGA_BASE_DIR

  if [ ! -e $DROONGA_BASE_DIR/catalog.json -o \
       ! -e $DROONGA_BASE_DIR/$NAME.yaml ]; then
    [ "$HOST" = "Auto Detect" ] &&
      HOST=$(determine_hostname)

    if [ "$HOST" = "" ]; then
      HOST=$(hostname)
      echo "********************** CAUTION!! **********************"
      echo "Installation process coudln't detect the hostname of"
      echo "this node, which is accessible from other nodes."
      echo "You may have to configure droonga-engine manually"
      echo "to refer a valid accessible hostname for this node,"
      echo "by following command line:"
      echo ""
      echo "  droonga-engine-configure --reset-config --reset-catalog"
      echo "*******************************************************"
    fi
    echo "This node is configured with a hostname $HOST."
  fi

  droonga-engine-configure --quiet \
    --host=$HOST

  chown -R $USER:$GROUP $DROONGA_BASE_DIR
}


guess_global_hostname() {
  if hostname -d > /dev/null 2>&1; then
    domain=$(hostname -d)
    hostname=$(hostname -s)
    if [ "$domain" != "" ]; then
      echo "$hostname.$domain"
      return 0
    fi
  fi
  echo ""
  return 1
}

determine_hostname() {
  global_hostname=$(guess_global_hostname)
  if [ "$global_hostname" != "" ]; then
    echo "$global_hostname"
    return 0
  fi

  address=$(hostname -i | \
            $sed -e "s/127\.[0-9]+\.[0-9]+\.[0-9]+//g" \
                 -e "s/  +/ /g" \
                 -e "s/^ +| +\$//g" |\
            cut -d " " -f 1)
  if [ "$address" != "" ]; then
    echo "$address"
    return 0
  fi

  echo ""
  return 1
}


install_rroonga() {
  # Install Rroonga globally from a public gem, because custom build
  # doesn't work as we expect for Droonga...
  if exist_command grndump; then
    current_version=$(grndump -v | cut -d " " -f 2)
    version_matcher=$(cat $NAME.gemspec | \
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

  tempdir=/tmp/install-$NAME
  mkdir $tempdir
  cd $tempdir

  if [ -d $NAME ]
  then
    cd $NAME
    install_rroonga
    git reset --hard
    git pull --rebase
    bundle update
  else
    git clone $REPOSITORY_URL
    cd $NAME
    install_rroonga
    bundle install
  fi
  rm -rf pkg
  bundle exec rake build
  gem install "pkg/*.gem" --no-ri --no-rdoc
}

install_service_script() {
  INSTALL_LOCATION=$1
  DOWNLOAD_URL=$SCRIPT_URL/$PLATFORM/$NAME
  curl -o $INSTALL_LOCATION $DOWNLOAD_URL
  chmod +x $INSTALL_LOCATION
}



# ====================== for Debian/Ubuntu ==========================

prepare_environment_in_debian() {
  apt-get update
  apt-get -y upgrade
  apt-get install -y ruby ruby-dev build-essential

  if [ "$VERSION" = "master" ]; then
    apt-get install -y git
  fi
}

register_service_in_debian() {
  pid_dir=/var/run/$NAME
  mkdir -p $pid_dir
  chown -R $USER:$GROUP $pid_dir

  install_service_script /etc/init.d/$NAME debian
  update-rc.d $NAME defaults
}

# ====================== /for Debian/Ubuntu =========================



# ========================= for CentOS 7 ============================

prepare_environment_in_centos() {
  yum update
  yum -y groupinstall development
  yum -y install ruby-devel

  if [ "$VERSION" = "master" ]; then
    yum -y install git
  fi
}

register_service_in_centos() {
  install_service_script /etc/rc.d/init.d/$NAME centos
  /sbin/chkconfig --add $NAME
}

# ========================= /for CentOS 7 ===========================



install() {
  echo "Preparing the environment..."
  prepare_environment_in_$PLATFORM

  echo ""
  if [ "$VERSION" = "master" ]; then
    echo "Installing $NAME from the git repository..."
    install_master
  else
    echo "Installing $NAME from RubyGems..."
    gem install droonga-engine --no-rdoc --no-ri
  fi

  prepare_user

  setup_configuration_directory

  echo ""
  echo "Registering $NAME as a service..."
  register_service_in_$PLATFORM

  echo ""
  echo "Successfully installed $NAME."
}

if [ "$EUID" != "0" ]; then
  echo "You must run this script as the root."
  exit 1
elif [ -e /etc/debian_version ] || [ -e /etc/debian_release ]; then
  PLATFORM=debian
elif [ -e /etc/centos-release ]; then
  PLATFORM=centos
else
  echo "Not supported platform. This script works only for Debian or CentOS."
  exit 255
fi

install

exit 0
