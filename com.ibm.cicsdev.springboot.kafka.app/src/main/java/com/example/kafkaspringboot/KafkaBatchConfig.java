/* Licensed Materials - Property of IBM                               */
/*                                                                    */
/* SAMPLE                                                             */
/*                                                                    */
/* (c) Copyright IBM Corp. 2016, 2026 All Rights Reserved             */
/*                                                                    */
/* US Government Users Restricted Rights - Use, duplication or        */
/* disclosure restricted by GSA ADP Schedule Contract with IBM Corp   */
/*                                                                    */
package com.ibm.cicsdev.springboot.kafka;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;


@Configuration
@EnableKafka
@ConfigurationProperties(prefix = "cics.transaction")
/**
 * Holds transaction mapping per Kafka topic.
 */
public class KafkaBatchConfig
{

    private Map<String, String> map = new HashMap<>();


    public Map<String, String> getMap()
    {
        return map;
    }


    public void setMap(Map<String, String> map)
    {
        this.map = map;
    }


    public String getTranIdForTopic(String topic)
    {
        return map.getOrDefault(topic, "CJSU");
    }


    /**
     * Create a batch-enabled container factory that reuses Spring Boot's auto-configured ConsumerFactory. <br>
     * Spring Boot already provides a default KafkaListenerContainerFactory from properties; we’re only adding a
     * dedicated one named "batchFactory" with setBatchListener(true).
     */
    @Bean(name = "batchFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> batchFactory(
        ConsumerFactory<String, String> consumerFactory)
    {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);

        // Enable batch delivery
        factory.setBatchListener(true);

        // Optional: normally 1 is fine, but let's assume scale
        factory.setConcurrency(3);

        // Keep other defaults from properties (ack mode, poll timeout, etc.)
        return factory;
    }
}

