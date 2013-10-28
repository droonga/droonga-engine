#!/bin/sh

base_dir=$(cd $(dirname $0); pwd)

# commands to remove temporary db
# commands to create temporary db

DROONGA_CATALOG=$base_dir/catalog.json \
  bundle exec fluentd \
    --config $base_dir/fluentd.conf &
FLUENTD_PID=$!

sleep 1

bundle exec fluent-cat -p 23003 droonga < $base_dir/ddl/watchdb.jsons 

bundle exec ruby $base_dir/benchmark-notify.rb

kill $FLUENTD_PID
wait $FLUENTD_PID

