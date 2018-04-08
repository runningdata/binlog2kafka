package com.alibaba.otter.canal.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.GetProperties;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class One {
    private static KafkaProducer<String, String> producer;
    private static CanalConnector connector = null;
    private static int debug = 0;

    public static void run() {
        // 创建链接
        GetProperties.getProperties();
        debug = GetProperties.debug;
        Properties props = new Properties();
        props.put("bootstrap.servers", GetProperties.servers);
        props.put("client.id", "oneProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        System.out.println("props is " + props);
        int batchSize = 1000;
        int emptyCount = 0;
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(GetProperties.ip,
                GetProperties.port), GetProperties.destination, GetProperties.username, GetProperties.password);

        connector.connect();
        if (!"".equals(GetProperties.filter)) {
            connector.subscribe(GetProperties.filter);
        } else {
            connector.subscribe();
        }

        connector.rollback();

        try {
            producer = new KafkaProducer<>(props);
            long bathch_num = 0;
            long msg_num = 0;
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    if (debug > 0) {
                        System.out.println("empty count : " + emptyCount);
                    }
                    if (debug > 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {

                        }
                    }
                } else {
                    emptyCount = 0;
                    bathch_num++;
                    msg_num += size;
                    if (debug > 0) {
                        System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                        System.out.printf("Total: [batch_num=%s,msg_num=%s] \n", bathch_num, msg_num);
                    }

                    if (syncEntry(message.getEntries())) {
                        connector.ack(batchId); // 提交确认
                    } else {
                        connector.rollback(batchId); // 处理失败, 回滚数据
                        System.err.println("fail for batchId" + batchId);
                    }
                }
            }
        } finally {
            if (connector != null) {
                connector.disconnect();
                connector = null;
            }
            if (connector != null) {
                producer.close();
                producer = null;
            }
        }
    }

    public static void main(String args[]) throws InterruptedException {
        while (true) {
            try {
                run();
            } catch (Exception e) {
                System.out.println("run error");
                Thread.sleep(3000);
                e.printStackTrace();
            }
        }
    }

    private static boolean syncEntry(List<Entry> entrys) {
        String topic = GetProperties.oneTopic;
        int no = 0;
        RecordMetadata metadata = null;
        boolean ret = true;
        long tt = new Date().getTime();
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            Map<String, Object> data = new HashMap<String, Object>();
            Map<String, Object> head = new HashMap<String, Object>();
            head.put("binlog_file", entry.getHeader().getLogfileName());
            head.put("binlog_pos", entry.getHeader().getLogfileOffset());
            head.put("db", entry.getHeader().getSchemaName());
            head.put("table", entry.getHeader().getTableName());
            head.put("type", eventType);
            head.put("ctime", tt);
            data.put("head", head);
//	        topic = entry.getHeader().getSchemaName() + "_" + entry.getHeader().getTableName();
            no = (int) entry.getHeader().getLogfileOffset();
            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    data.put("before", makeColumn(rowData.getBeforeColumnsList()));
                } else if (eventType == EventType.INSERT) {
                    data.put("after", makeColumn(rowData.getAfterColumnsList()));
                } else {
                    data.put("before", makeColumn(rowData.getBeforeColumnsList()));
                    data.put("after", makeColumn(rowData.getAfterColumnsList()));
                }
                String text = JSON.toJSONString(data);
                try {
                    String key = head.get("db") + "___" + head.get("table") + "___" + no;
                    metadata = producer.send(new ProducerRecord<>(topic, key, text)).get();
                    if (metadata == null) {
                        ret = false;
                    }
                    if (debug > 0) {
                        System.out.println("Sent message: (" + topic + "," + key + ", " + text + ")");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    ret = false;
                }
            }
            data.clear();
            data = null;
        }
        return ret;
    }

    private static List<Map<String, Object>> makeColumn(List<Column> columns) {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (Column column : columns) {
            Map<String, Object> one = new HashMap<String, Object>();
//			one.put("type", column.getMysqlType());
            one.put("name", column.getName());
            one.put("value", column.getValue());
            one.put("update", column.getUpdated());
            list.add(one);
        }
        return list;
    }

}