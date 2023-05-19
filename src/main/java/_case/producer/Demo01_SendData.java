package _case.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author shaco
 * @version Flink 1.13.6，Flink CDC 2.2.1
 * @create 2023-05-17 21:39
 * @desc 生产者API：异步不带回调数据发送；带回调数据发送；同步不带回调数据发送；同步带回调数据发送
 */
public class Demo01_SendData {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // TODO 0、Kafka生产者配置：通过ProducerConfig对象设置生产者的配置，并装入Properties集合中
        // 创建Properties集合
        Properties producerProp = new Properties();
        // 配置Kafka集群连接地址，一般配置集群中的两个节点的访问地址和端口号，必须
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop132:9092,hadoop133:9092");

        // 配置数据的序列化方式，Kafka中的数据存储和反序列化一般使用字符串
        // Kafka中，数据一般以key-value的形式存在，所以需要分别配置key和value的序列化方式
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // TODO 1、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProp);

        // TODO 2、发送数据
        for (int i = 1; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first", "hello world " + i);

            // TODO 不带回调的异步数据发送方式，只需要调用send()方法将数据发送出去即可
            producer.send(record);

            // TODO 带回调的异步数据发送方式，需要传入第二个Callback类型的参数
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) { // 没有异常，说明数据发送成功
                        String topic = metadata.topic();
                        int partition = metadata.partition();
                        long timestamp = metadata.timestamp();
                        System.out.println("topic: " + topic + "，分区：" + partition + "，写入Kafka的时间：" + timestamp);
                    }
                }
            });

            // TODO 不带回调的同步数据发送方式，只需要基于异步发送方式，再调用get()方法即可
            producer.send(record).get(); // 注意要进行异常处理

            // TODO 带回调的同步数据发送方式，还需要传入第二个参数
            producer.send(record, new Callback() {
                @Override
                
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) { // 没有异常，说明数据发送成功
                        String topic = metadata.topic();
                        int partition = metadata.partition();
                        long timestamp = metadata.timestamp();
                        System.out.println("topic: " + topic + "，分区：" + partition + "，写入Kafka的时间：" + timestamp);
                    }
                }
            }).get();
        }

        // TODO 3、关闭资源
        producer.close();
    }
}
