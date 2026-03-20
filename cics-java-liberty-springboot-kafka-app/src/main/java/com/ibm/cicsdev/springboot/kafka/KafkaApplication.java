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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * Spring Boot Entry Point for Kafka Consumer Application.
 */
@SpringBootApplication
public class KafkaApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(KafkaApplication.class, args);
    }
}
