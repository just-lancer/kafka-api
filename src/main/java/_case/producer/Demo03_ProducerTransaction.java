package _case.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

/**
 * Author: shaco
 * Date: 2023/5/20
 * Desc: 生产者事务：开启事务发送数据，如果遇到异常，回滚事务
 */
public class Demo03_ProducerTransaction {
    public static void main(String[] args) {
        // 0、生产者配置
        Properties prop = new Properties();

        // 配置Kafka集群连接地址
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop132:9092");

        //配置key-value序列化方式
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消息累加器RecoderAccumulator的大小，设置为64M，该参数的配置单位为字节
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG,64*1024*1024);

        // 配置数据批ProducerBatch大小，配置为32K，参数的单位是字节
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,32*1024);

        // 配置ProducerBatch攒批，也就是等待数据的时间，配置为300 ms
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 300);

        // 配置ACK应答级别
        prop.put(ProducerConfig.ACKS_CONFIG,"-1");

        // 使用并配置压缩方式
        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // TODO 配置事务ID，开启生产者事务必须配置，只有一个要求，Kafka集群中唯一即可，取值随意
        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction_id");

        // 配置Kafka生产者事务超时时间，默认超时时间1分钟，配置单位毫秒
        prop.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,3 * 60 * 1000);

        // 1、创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        // 开启事务
        kafkaProducer.beginTransaction();

        try{
            // 发送数据
            for (int i = 1; i <= 5;i++){
                if (i!=3){
                    ProducerRecord<String, String> record = new ProducerRecord<>("first", "hello world " + i);
                    kafkaProducer.send(record);
                }
            }

            // 提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            // 遇到异常，终止事务
            kafkaProducer.abortTransaction();
        }finally {
            // 关闭资源
            kafkaProducer.close();
        }
    }
}
