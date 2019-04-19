package com.github.ers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Consumer.class);

        CountDownLatch latch = new CountDownLatch(1);

        Properties consumerProperties = new Config().getConsumerProperties();

        //Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                consumerProperties.getProperty("bootStrapServer"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                consumerProperties.getProperty("groupId"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                consumerProperties.getProperty("autoOffset"));

        //Creating the consumer thread
        Runnable consumerRunnable = new ConsumerRunnable(
                latch,
                consumerProperties.getProperty("topic"),
                properties);

        Thread thread = new Thread(consumerRunnable);

        thread.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited!");
        }));
    }
}
