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

register_service() {
  local NAME=$1
  local USER=$2
  local GROUP=$3

  #TODO: we should migrate to systemd in near future...

  local pid_dir=/run/$NAME
  mkdir -p $pid_dir
  chown -R $USER:$GROUP $pid_dir

  curl -o /etc/rc.d/init.d/$NAME $(download_url "install/centos/$NAME")
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to download service script!"
    exit 1
  fi

  chmod +x /etc/rc.d/init.d/$NAME
  /sbin/chkconfig --add $NAME
}
