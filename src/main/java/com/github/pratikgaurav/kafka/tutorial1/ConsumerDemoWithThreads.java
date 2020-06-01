package com.github.pratikgaurav.kafka.tutorial1;

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
        new ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){ }

    private void run(){
        String bootstrapServers = "127.0.0.1:9092";
        String group_id = "my-sixth-application";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        //Latch for dealing with multiple Threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create the Consumer Runnable
        logger.info("Creating Consumer Thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, group_id, topic, latch);

        //Start the Thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has Exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is Interrupted", e);
        }finally {
            logger.info("Application is Closing");
        }
    }
    public class ConsumerRunnable implements Runnable{

        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        public ConsumerRunnable(String bootstrapServers, String group_id, String topic, CountDownLatch latch){
            this.latch = latch;
            //Create Consumer Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //Create Consumer
            consumer = new KafkaConsumer<String, String>(properties);
            //Subscribe a Consumer to a Topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //Poll for New Data
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100)); //New in Kafka 2.0.0
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("\nKey: " + record.key() + ", Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");
            }finally {
                consumer.close();
                //Tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //The wakeup() method is a special method to interrupt consumer.poll()
            //It will throw an exception called WakeUpException
            consumer.wakeup();
        }
    }
}
