#!/bin/sh

sed -e "s/{CANAL_SERVER_IP}/${CANAL_SERVER_IP}/g" \
    -e "s/{CANAL_SERVER_PORT}/${CANAL_SERVER_PORT}/g" \
    -e "s/{DESTINATION}/${DESTINATION}/g" \
    -e "s/{FILTER}/${FILTER}/g" \
    -e "s/{KAFKA_SERVERS}/${KAFKA_SERVERS}/g" \
    -e "s/{KAFKA_TOPIC}/${KAFKA_TOPIC}/g" \
    -e "s/{USER_NAME}/${USER_NAME}/g" \
    -e "s/{PASSWORD}/${PASSWORD}/g" \
    /SysConfig.properties_template > /SysConfig.properties
java -cp will_sync-1.0.2.jar com.alibaba.otter.canal.process.One