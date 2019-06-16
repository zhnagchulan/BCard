package com.wanda.mob.spark.streaming.kafka.offsetsv1;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by siyuan.tao on 2017/5/25.
 * kafka offset 工具类，获取 topic 各 partition 上的最小和最大 offset.
 */
public class MyKafkaOffsetUtils {
    private static MyKafkaOffsetUtils instance;
    private final int TIMEOUT = 100000;
    private final int BUFFER_SIZE = 64 * 1024;

    private MyKafkaOffsetUtils() {}

    /**
     * @return 单实例
     */
    public static synchronized MyKafkaOffsetUtils getInstance() {
        if (instance == null) {
            synchronized (MyKafkaOffsetUtils.class) {
                if (instance == null)
                    instance = new MyKafkaOffsetUtils();
            }
        }
        return instance;
    }

    /**
     * @param brokerList
     * @param topics
     * @param groupId
     * @return 各 partition 当前最大的 offset
     */
    public Map<TopicAndPartition, Long> getLastOffset(String brokerList, List<String> topics, String groupId) {
        Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();
        Map<TopicAndPartition, Broker> topicAndPartitionBrokerMap = MyKafkaOffsetUtils.getInstance().findLeader(brokerList, topics);

        for (Map.Entry<TopicAndPartition, Broker> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap.entrySet()) {
            // get leader broker
            Broker leaderBroker = topicAndPartitionBrokerEntry.getValue();
            SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), TIMEOUT, BUFFER_SIZE, groupId);
            long readOffset = getTopicAndPartitionLastOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey(), groupId);
            topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);
        }
        return topicAndPartitionLongMap;
    }

    /**
     * @param brokerList kafka brokers, 格式 host1:port1,host2:port2,...
     * @param topics topic list
     * @param groupId
     * @return 各 partition 当前最小的 offset
     */
    public Map<TopicAndPartition, Long> getEarliestOffset(String brokerList, List<String> topics, String groupId) {
        Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();
        Map<TopicAndPartition, Broker> topicAndPartitionBrokerMap = MyKafkaOffsetUtils.getInstance().findLeader(brokerList, topics);
        for (Map.Entry<TopicAndPartition, Broker> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap.entrySet()) {
            // get leader broker
            Broker leaderBroker = topicAndPartitionBrokerEntry.getValue();
            SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), TIMEOUT, BUFFER_SIZE, groupId);

            long readOffset = getTopicAndPartitionEarliestOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey(), groupId);
            topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);
        }
        return topicAndPartitionLongMap;
    }

    /**
     * get TopicAndPartition
     * @param brokerList kafka brokers, 格式 host1:port1,host2:port2,...
     * @param topics topic list
     * @return topicAndPartitions
     */
    private Map<TopicAndPartition, Broker> findLeader(String brokerList, List<String> topics) {
        // get broker's url array
        String[] brokerUrlArray = getBorkerUrlFromBrokerList(brokerList);
        // get broker's port map
        Map<String, Integer> brokerPortMap = getPortFromBrokerList(brokerList);

        // create array list of TopicAndPartition
        Map<TopicAndPartition, Broker> topicAndPartitionBrokerMap = Maps.newHashMap();

        for (String broker : brokerUrlArray) {
            SimpleConsumer consumer = null;
            try {
                // new instance of simple Consumer
                consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFER_SIZE,
                        "leaderLookup" + new Date().getTime());
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        TopicAndPartition topicAndPartition = new TopicAndPartition(item.topic(), part.partitionId());
                        topicAndPartitionBrokerMap.put(topicAndPartition, part.leader());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        return topicAndPartitionBrokerMap;
    }

    /**
     * get last offset
     * @param consumer SimpleConsumer
     * @param topicAndPartition TopicAndPartition
     * @param clientName String
     * @return
     */
    private long getTopicAndPartitionLastOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition, String clientName) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));

        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching Offset Data of the Broker: "
                            + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
            return 0;
        }
        long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        return offsets[0];
    }

    /**
     * get earliest offset
     * @param consumer
     * @param topicAndPartition
     * @param clientName
     * @return
     */
    private long getTopicAndPartitionEarliestOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition, String clientName) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching Offset Data of the Broker: "
                            + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
            return 0;
        }
        long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        return offsets[0];
    }

    /**
     * get broker url
     * @param brokerList
     * @return
     */
    private String[] getBorkerUrlFromBrokerList(String brokerList) {
        String[] brokers = brokerList.split(",");
        for (int i = 0; i < brokers.length; i++) {
            brokers[i] = brokers[i].split(":")[0];
        }
        return brokers;
    }

    /**
     * get broker url and port
     * @param brokerList
     * @return
     */
    private Map<String, Integer> getPortFromBrokerList(String brokerList) {
        Map<String, Integer> map = new HashMap();
        String[] brokers = brokerList.split(",");
        for (String item : brokers) {
            String[] itemArr = item.split(":");
            if (itemArr.length > 1) {
                map.put(itemArr[0], Integer.parseInt(itemArr[1]));
            }
        }
        return map;
    }

    public static void main(String[] args) {
        List<String> topics = Lists.newArrayList();
        topics.add("voc_all_detail0517");

        Map<TopicAndPartition, Long> topicAndPartitionLongMap = MyKafkaOffsetUtils.getInstance().
                getEarliestOffset("voc-hadoop1-uat:9092", topics,"pj-test-20170516");
        for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
            System.out.println(entry.getKey().topic() + ", partition: " + entry.getKey().partition() + ", smallest offset: " + entry.getValue());
        }

        System.out.println("==============================================");

        topicAndPartitionLongMap = MyKafkaOffsetUtils.getInstance().
                getLastOffset("voc-hadoop1-uat:9092", topics,"pj-test-20170516");
        for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
            System.out.println(entry.getKey().topic() + ", partition: " + entry.getKey().partition() + ", largest offset: " + entry.getValue());
        }
    }
}