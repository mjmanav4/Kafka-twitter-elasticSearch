package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        //    System.out.println("hello");

new ConsumerDemoWithThreads().run();


    }

    private ConsumerDemoWithThreads(){

    }
    private   void  run(){
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupid = "my-sixth-application";
        String topic = "first_topic";

        logger.info("Creating the consumer thread");
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch,topic,bootstrapServers,groupid);

    Thread mythread = new Thread(myConsumerThread);
    mythread.start();

    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
        logger.info("Caught shutdown hook");
        ((ConsumerThread) myConsumerThread).shutdown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("Application has existed");
    }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("APPLICATION GOT interrupted",e);
        }finally {
            logger.info("Application in closing");
        }
    }


    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;

        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch latch, String topic,String bootstrapServers, String groupid) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("value {} key{} par {} off {}", record.value(), record.key(), record.partition(), record.offset());
                    }

                }
            }catch(WakeupException e){
                logger.info("Received Shutdown signal ");
            }finally {
                consumer.close();
                latch.countDown();
            }

        }

        public  void shutdown(){
            consumer.wakeup();
        }

    }
}
