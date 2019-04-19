package com.github.ers.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

    Logger logger = LoggerFactory.getLogger(Config.class.getName());

    private static final String CONSUMER_PROP_FILE_NAME = "consumer.properties";

    private Properties properties;

    public java.util.Properties getConsumerProperties(){
        return this.properties == null ? readProperties() : this.properties;
    }

    private  java.util.Properties readProperties(){
        this.properties = new Properties();
        try {
            this.properties.load(new FileInputStream(CONSUMER_PROP_FILE_NAME));
        } catch (IOException e) {
            logger.error("Erro ao carregar 'consumer.properties'");
        }
        return this.properties;
    }
}
