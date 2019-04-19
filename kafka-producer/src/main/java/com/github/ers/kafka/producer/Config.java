package com.github.ers.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

    Logger logger = LoggerFactory.getLogger(Config.class.getName());

    private static final String PRODUCER_PROP_FILE_NAME = "producer.properties";

    private Properties properties;

    public java.util.Properties getProducerProperties(){
        return this.properties == null ? readProperties() : this.properties;
    }

    public java.util.Properties readProperties(){
         this.properties = new Properties();
        try {
            this.properties.load(new FileInputStream(PRODUCER_PROP_FILE_NAME));
        } catch (IOException e) {
            logger.error("Erro ao carregar 'producer.properties'");
        }
        return this.properties;
    }

}
