package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        //    System.out.println("hello");
        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers="127.0.0.1:9092";

        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // assign
        TopicPartition partitionToReadForm = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadForm));

        long offsetToReadForm =  15L;

        int numOfMessagesToRead =5;
        int numOfMessagesReadSoFar = 0;
        Boolean keepReading = true;
        // seek
        consumer.seek(partitionToReadForm,offsetToReadForm);

        while(keepReading){

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record:records){
                numOfMessagesReadSoFar+=1;
                logger.info("value {} key{} par {} off {}",record.value(),record.key(),record.partition(),record.offset());




                if(numOfMessagesReadSoFar>=5) {
                    keepReading = false;
                    break;
                }
            }

        }
    }
}
