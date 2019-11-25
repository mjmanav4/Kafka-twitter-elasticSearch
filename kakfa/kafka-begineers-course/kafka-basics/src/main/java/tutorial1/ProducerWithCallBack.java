package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world2");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("TOPIC: {}", recordMetadata.topic() + "\n" +
                            "Partition: {}", recordMetadata.partition() + "\n" +
                            "Offset: {}", recordMetadata.offset() + "\n" +
                            "TimeStamp: {}", recordMetadata.timestamp() + "\n"
                    );
                } else {
                    logger.error("ERROR ", e);
                }
            }
        });
        producer.flush();
        producer.close();
        System.out.println("HELLO WORLD");

    }
}
