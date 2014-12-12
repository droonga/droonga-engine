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
DOWNLOAD_URL_BASE=https://raw.githubusercontent.com/droonga/$NAME
REPOSITORY_URL=https://github.com/droonga/$NAME.git
USER=$NAME
GROUP=droonga
DROONGA_BASE_DIR=/home/$USER/droonga
TEMPDIR=/tmp/install-$NAME

: ${VERSION:=release}
: ${HOST:=Auto Detect}
: ${PORT:=10031}

case $(uname) in
  Darwin|*BSD|CYGWIN*) sed="sed -E" ;;
  *)                   sed="sed -r" ;;
esac

ensure_root() {
  if [ "$EUID" != "0" ]; then
    echo "You must run this script as the root."
    exit 1
  fi
}

guess_platform() {
  if [ -e /etc/debian_version ] || [ -e /etc/debian_release ]; then
    echo "debian"
    return 0
  elif [ -e /etc/centos-release ]; then
    echo "centos"
    return 0
  fi
  return 1
}

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

exist_yum_repository() {
  if ! yum --enablerepo=$1 repolist; then
    return 1
  fi
  yum --enablerepo=$1 repolist | grep --quiet "$1"
}

exist_user() {
  id "$1" > /dev/null 2>&1
}

prepare_user() {
  echo ""
  echo "Preparing the user..."

  groupadd $GROUP

  if ! exist_user $USER; then
    useradd -m $USER
  fi

  usermod -G $GROUP $USER
  return 0
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

  # we should use --no-prompt instead of --quiet, for droonga-engine 1.0.8 and later.
  droonga-engine-configure --quiet \
    --host=$HOST --port=$PORT
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to configure $NAME!"
    exit 1
  fi

  chown -R $USER:$GROUP $DROONGA_BASE_DIR
}


guess_global_hostname() {
  if hostname -d > /dev/null 2>&1; then
    local domain=$(hostname -d)
    local hostname=$(hostname -s)
    if [ "$domain" != "" ]; then
      echo "$hostname.$domain"
      return 0
    fi
  fi
  echo ""
  return 1
}

determine_hostname() {
  local global_hostname=$(guess_global_hostname)
  if [ "$global_hostname" != "" ]; then
    echo "$global_hostname"
    return 0
  fi

  local address=$(hostname -i | \
                  $sed -e "s/127\.[0-9]+\.[0-9]+\.[0-9]+//g" \
                       -e "s/[0-9a-f:]+%[^ ]+//g" \
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

download_url() {
  if [ "$VERSION" = "master" ]; then
    echo "$DOWNLOAD_URL_BASE/master/$1"
  else
    echo "$DOWNLOAD_URL_BASE/v$(installed_version)/$1"
  fi
}

installed_version() {
  $NAME --version | cut -d " " -f 2
}


install_rroonga() {
  # Install Rroonga globally from a public gem, because custom build
  # doesn't work as we expect for Droonga...
  if exist_command grndump; then
    local current_version=$(grndump -v | cut -d " " -f 2)
    local version_matcher=$(cat $NAME.gemspec | \
                            grep rroonga | \
                            cut -d "," -f 2 | \
                            cut -d '"' -f 2)
    local compared_version=$(echo "$version_matcher" | \
                             cut -d " " -f 2)
    local operator=$(echo "$version_matcher" | cut -d " " -f 1)
    local compare_result=$(ruby -e "puts('$current_version' $operator '$compared_version')")
    if [ "$compare_result" = "true" ]; then return 0; fi
  fi
  gem install rroonga --no-ri --no-rdoc
}

install_master() {
  gem install bundler --no-ri --no-rdoc

  cd $TEMPDIR

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
    bundle install --path vendor/
  fi
  rm -rf pkg
  bundle exec rake build
  gem install "pkg/*.gem" --no-ri --no-rdoc
}



# ====================== for Debian/Ubuntu ==========================
prepare_environment_in_debian() {
  local use_groonga_package=no
  if apt-cache policy | grep --quiet groonga; then
    use_groonga_package=yes
  else
    if [ "$(lsb_release -i -s)" = "Ubuntu" ]; then
      add-apt-repository -y ppa:groonga/ppa
      use_groonga_package=yes
    else
      local groonga_list=/etc/apt/sources.list.d/groonga.list
      echo "deb http://packages.groonga.org/debian/ wheezy main" >> $groonga_list
      echo "deb-src http://packages.groonga.org/debian/ wheezy main" >> $groonga_list
      apt-get update
      apt-get install -y --allow-unauthenticated groonga-keyring
      use_groonga_package=yes
    fi
  fi

  apt-get update
  apt-get install -y curl ruby ruby-dev build-essential

  if [ "$use_groonga_package" = "yes" ]; then
    apt-get install -y libgroonga-dev
  fi

  if [ "$VERSION" = "master" ]; then
    apt-get install -y git
  fi
}
# ====================== /for Debian/Ubuntu =========================



# ========================= for CentOS 7 ============================
prepare_environment_in_centos() {
  local use_groonga_package=no
  if ! exist_yum_repository groonga; then
    rpm -ivh http://packages.groonga.org/centos/groonga-release-1.1.0-1.noarch.rpm

    # disable it by default!
    groonga_repo=/etc/yum.repos.d/groonga.repo
    backup=/tmp/$(basename $groonga_repo).bak
    mv $groonga_repo $backup
    cat $backup | $sed -e "s/enabled=1/enabled=0/" \
      > $groonga_repo
  fi

  if exist_yum_repository groonga; then
    use_groonga_package=yes
    yum -y --enablerepo=groonga makecache
  else
    yum -y makecache
  fi
  yum -y groupinstall development
  yum -y install curl ruby-devel

  if [ "$use_groonga_package" = "yes" ]; then
    yum -y --enablerepo=groonga install groonga-devel
  fi

  if [ "$VERSION" = "master" ]; then
    yum -y install git
  fi
}
# ========================= /for CentOS 7 ===========================



install() {
  mkdir -p $TEMPDIR

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

  if ! exist_command droonga-engine; then
    echo "ERROR: Failed to install $NAME!"
    exit 1
  fi

  curl -o $TEMPDIR/functions.sh $(download_url "install/$PLATFORM/functions.sh")
  if ! source $TEMPDIR/functions.sh; then
    echo "ERROR: Failed to download post-installation script!"
    exit 1
  fi
  if ! exist_command register_service; then
    echo "ERROR: Downloaded post-installation script is broken!"
    exit 1
  fi

  prepare_user

  setup_configuration_directory

  echo ""
  echo "Registering $NAME as a service..."
  # this function is defined by the downloaded "functions.sh"!
  register_service $NAME $USER $GROUP

  echo ""
  echo "Successfully installed $NAME."
}


ensure_root

PLATFORM=$(guess_platform)
if [ "$PLATFORM" = "" ]; then
  echo "Not supported platform."
  exit 255
fi

install

exit 0
