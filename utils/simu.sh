#!/bin/bash
# Usage 
# ./simu.sh <threads> <max events to collect>
#

trap "kill 0" SIGINT SIGTERM EXIT
set -x

#Default 1 thread
SIMULTANEOUS=${1:-1}
NUMBER=${2:-1}

#rm -f out.txt
#touch out.txt
cd ..
for i in {1..1}; do
#./run.sh $SIMULTANEOUS -1 com.hortonworks.streaming.impl.domain.wellsfargo.Securities com.hortonworks.streaming.impl.collectors.DefaultEventCollector > /dev/null &
./run.sh $SIMULTANEOUS $NUMBER com.hortonworks.streaming.impl.domain.wellsfargo.Securities com.hortonworks.streaming.impl.collectors.JmsEventCollector > /dev/null &
done
wait
echo "done"
