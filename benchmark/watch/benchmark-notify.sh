#!/bin/sh

base_dir=$(cd $(dirname $0); pwd)

rm -rf $base_dir/watch
mkdir -p $base_dir/watch

DROONGA_CATALOG=$base_dir/catalog.json \
  bundle exec fluentd \
    --config $base_dir/fluentd.conf &
FLUENTD_PID=$!

sleep 1

bundle exec ruby $base_dir/benchmark-notify.rb "$@"

kill $FLUENTD_PID
wait $FLUENTD_PID

