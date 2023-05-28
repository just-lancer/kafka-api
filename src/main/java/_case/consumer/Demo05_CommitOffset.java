package _case.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Author: shaco
 * Date: 2023/5/27
 * Desc: 手动提交offset，自动提交offset
 */
public class Demo05_CommitOffset {
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

        // TODO 设置手动提交offset，每消费一批数据，提交一次offset。默认值是true，表示自动提交offset
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        // 1、创建一个消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);

        // TODO 2、订阅消费的主题，可以一次订阅一个主题，也可以一次订阅多个主题
        // 该方式将消费订阅的主题的所有分区的数据
        ArrayList<String> subscribeTopics = new ArrayList<>();
        subscribeTopics.add("first");
        kafkaConsumer.subscribe(subscribeTopics);

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

            // TODO 手动提交offset
            // 异步提交offset
            kafkaConsumer.commitAsync();
            // 同步提交offset
            // kafkaConsumer.commitSync();
        }
    }
}
