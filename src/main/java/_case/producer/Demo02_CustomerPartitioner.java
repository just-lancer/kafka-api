package _case.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author shaco
 * @version Flink 1.13.6，Flink CDC 2.2.1
 * @create 2023-05-18 19:59
 * @desc 自定义生产者分区器
 */
public class Demo02_CustomerPartitioner implements Partitioner {
    // TODO 实现三个抽象方法：partition()：定义分区逻辑；close()：关闭资源；configure()：不用管
    // 各参数含义：
    // @param topic         topic，即主题
    // @param key           消息的key
    // @param keyBytes      消息的key经过序列化之后的字节数组
    // @param value         消息的value
    // @param valueBytes    消息的value经过序列化之后的字节数组
    // @param cluster       Kafka集群的元数据，可用于查看主题、分区等元数据信息
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String message = value.toString();
        int partition;
        if (message.contains("hello")) {
            partition = 0;
        } else {
            partition = 1;
        }
        return partition;
    }

    // 用于关闭资源
    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    public static void main(String[] args) {
        // 利用自定义分区器，进行数据发送
        // 0 Kafka生产者配置
        Properties properties = new Properties();

        // Kafka连接地址，以及序列化器
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop132:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // TODO 为了让Kafka使用自定义分区器进行数据发送，需要利用ProducerConfig对象进行配置
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "_case.producer.Demo02_CustomerPartitioner");

        // 1、创建Kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2、发送数据
        String[] strings = {"hello", "hello", "hello", "hello", "world"};
        for (int i = 0; i < strings.length; i++) {
            ProducerRecord<String, String> message = new ProducerRecord<>("first", strings[i] + i);
            producer.send(message, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("topic: " + metadata.topic() + "; partition: " + metadata.partition());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        // 3、关闭资源
        producer.close();
    }
}
