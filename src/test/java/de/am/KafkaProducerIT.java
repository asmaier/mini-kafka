package de.am;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import static org.junit.Assert.*;

/**
 * For online documentation
 * see
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/utils/TestUtils.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/admin/TopicCommand.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/admin/TopicCommandTest.scala
 */
public class KafkaProducerIT {

    private int brokerId = 0;
    private String topic = "test";

    @Test
    public void producerTest() throws InterruptedException {

        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
        ZkClient zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        String [] arguments = new String[]{"--topic", topic, "--partitions", "1","--replication-factor", "1"};
        // create topic
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(arguments));

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + port);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // send message
        KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, "test-message".getBytes(StandardCharsets.UTF_8));

        List<KeyedMessage> messages = new ArrayList<KeyedMessage>();
        messages.add(data);

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        zkClient.delete("/consumers/group0");

        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if(iterator.hasNext()) {
            String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
            System.out.println(msg);
            assertEquals("test-message", msg);
        } else {
            fail();
        }

        // cleanup
        consumer.shutdown();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
}