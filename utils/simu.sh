#!/bin/bash

trap "kill 0" SIGINT SIGTERM EXIT
set -x

THREADS=$1
THREADS=${THREADS:-3}

rm -f out.txt
touch out.txt
cd ..
for i in {1..$THREADS}; do
./run.sh 1 -1 com.hortonworks.streaming.impl.domain.wellsfargo.SdrReport com.hortonworks.streaming.impl.collectors.DefaultEventCollector >> utils/out.txt &
done
wait
echo "done"
