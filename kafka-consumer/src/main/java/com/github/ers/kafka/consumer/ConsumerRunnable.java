package com.github.ers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable{

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Integer durationToComplete = 100;
    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
    final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:MM:SS");

    public ConsumerRunnable(CountDownLatch latch, String topic, Properties properties){
        this.latch = latch;

        //Consumer
        this.consumer = new KafkaConsumer<>(properties);

        //Subscribe consumer
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try{
            //Poll for data
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(durationToComplete));
                for (ConsumerRecord<String, String> record: records){
                    logger.info(getMessageLog(record)
                    );
                }
            }
        } catch (WakeupException e){
            logger.info("Received shutdown signal!");
        }finally {
            consumer.close();
            latch.countDown();
        }

    }

    private String getMessageLog(ConsumerRecord<String, String> record) {
        return "Message received! \n" +
                "*|topic: " + record.topic() +
                " |key: " + record.key()+
                " |partition : " + record.partition() +
                " |offset: " + record.offset() +
                " |timestamp: " + dateFormat.format(new Date(record.timestamp())) +
                " | \n" +
                "*|message: " + record.value();
    }

    public void shutdown(){
        //Interrupt consumer.poll() throwing a WakeUpException
        consumer.wakeup();
    }
}
