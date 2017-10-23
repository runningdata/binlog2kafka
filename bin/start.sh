#!/bin/sh
java -cp will_sync-1.0.2.jar \
 -DCANAL_SERVER_IP=${CANAL_SERVER_IP} \
 -DCANAL_SERVER_PORT=${CANAL_SERVER_PORT} \
 -DDESTINATION=${DESTINATION} \
 -DFILTER=${FILTER} \
 -DKAFKA_SERVERS=${KAFKA_SERVERS} \
 com.alibaba.otter.canal.process.One