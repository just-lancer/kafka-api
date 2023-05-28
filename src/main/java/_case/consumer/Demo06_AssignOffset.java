package _case.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Author: shaco
 * Date: 2023/5/27
 * Desc: 指定offset进行数据消费
 */
public class Demo06_AssignOffset {
    public static void main(String[] args) {
        // 0、消费者配置
        Properties prop = new Properties();

        // 配置Kafka集群连接地址，必选项
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop132:9092");

        // 配置key和value的反序列化器，必选项
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // TODO 配置消费者组名，必选项
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        // 1、创建一个消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);

        // TODO 2、订阅消费的主题，可以一次订阅一个主题，也可以一次订阅多个主题
        // 该方式将消费订阅的主题的所有分区的数据
        ArrayList<String> subscribeTopics = new ArrayList<>();
        subscribeTopics.add("first");
        kafkaConsumer.subscribe(subscribeTopics);

        // TODO 指定offset进行数据消费
        // 创建一个集合用来存储消费者消费的分区
        Set<TopicPartition> topicPartitions = new HashSet<>();

        // 由于消费者组初始化流程较为繁复，有可能代码执行到这里，Kafka集群中，消费者组还没有初始化完成，消费者分区分配策略还没能执行完成，所以需要进行逻辑判断
        while (topicPartitions.size() == 0){
            // 如果不能获取到消费者所消费的分区，那么一直进分区获取，并判断
            topicPartitions = kafkaConsumer.assignment();
        }

        // 为每个分区指定offset的消费位置，每个分区都从100的位置开始消费
        for (TopicPartition topicPartition : topicPartitions) {
            kafkaConsumer.seek(topicPartition, 100);
        }

        // 3、消费数据：当消费到"stop"时，停止消费
        boolean isflag = true;
        while (isflag){
            // 每隔一秒进行一次拉取
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            // 将消费到的数据打印在控制台上
            // 判断消费到的数据有没有stop
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                if ("stop".equals(consumerRecord.value())){
                    isflag = false;
                }

                System.out.println(consumerRecord);
            }
        }
    }
}
