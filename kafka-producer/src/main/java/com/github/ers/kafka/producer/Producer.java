package com.github.ers.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:MM:SS");

        Properties producerProperties = new Config().getProducerProperties();

        //Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                producerProperties.getProperty("bootstrapServer"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10 ;i++) {

            //Producer record
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                    producerProperties.getProperty("topic")
                    , "Java producer msg " + i);

            //Send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info(getMetadataLog(recordMetadata, dateFormat, record)
                        );

                    } else {
                        logger.error("Error while producing message: ", e);
                    }
                }
            });
            producer.flush();
        }
        producer.close();
    }

    private static String getMetadataLog(RecordMetadata recordMetadata, SimpleDateFormat dateFormat,
                                         ProducerRecord<String, String> record) {
        return "Message produced! Metadata received! \n" +
                "*|topic: " + recordMetadata.topic() +
                " |partition : " + recordMetadata.partition() +
                " |offset: " + recordMetadata.offset() +
                " |timestamp: " + dateFormat.format(new Date(recordMetadata.timestamp())) +
                " | \n" +
                "*|message: " + record.value();
    }
}
