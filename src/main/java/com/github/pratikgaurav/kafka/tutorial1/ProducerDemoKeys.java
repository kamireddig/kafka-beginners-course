package com.github.pratikgaurav.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "Hello World" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            //Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key " + key);  //Log the Key

            //Send Data - asynchronous
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time a Record is successfully sent or an Exception is thrown
                    if (e == null) {
                        //The Record was Successfully sent
                        logger.info("\nReceived New Metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while Producing", e);
                    }
                }
            }).get(); //Blocks the .send() to make it Synchronous. DO NOT use in Production
        }
            //Flush Data
            producer.flush();
            //Flush and Close Producer
            producer.close();
    }
}
