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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import com.ibm.cics.server.CICSTransactionRunnable;
import com.ibm.cics.server.InvalidRequestException;
import com.ibm.cics.server.Task;

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedExecutorService;


/**
 * KafkaMessageProcessor executes incoming Kafka messages asynchronously in a CICS transaction context.
 *
 * <p>
 * Each message is wrapped in a CICSTransactionRunnable so it runs under the CICS Task environment and automatically
 * associates with the proper transaction ID based on the topic.
 * </p>
 */
@Service
public class KafkaMessageProcessor
{
    private static final Logger LOG = Logger.getLogger(KafkaMessageProcessor.class.getName());

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;

    private final KafkaBatchConfig config;

    //LoginManager manager = new LoginManager();

    public KafkaMessageProcessor(KafkaBatchConfig myConfig)
    {
        this.config = myConfig;
    }


    /**
     * Processes a Kafka message asynchronously.
     *
     * @param record
     */
    public void processAsynchronous(ConsumerRecord<String, String> record)
    {
        LOG.fine(() -> "Received message from topic " + record.topic() + ": " + record.value());
        executor.submit(new KafkaCICSTransactionRunnable(record, config));
    }


    /**
     * Runnable wrapper that executes a Kafka message within a CICS transaction.
     */
    private static class KafkaCICSTransactionRunnable implements CICSTransactionRunnable
    {
        private final ConsumerRecord<String, String> record;
        private final KafkaBatchConfig config;


        public KafkaCICSTransactionRunnable(ConsumerRecord<String, String> record, KafkaBatchConfig config)
        {
            this.record = record;
            this.config = config;
        }


        @Override
        public void run()
        {

            Task task = Task.getTask();
            if (task == null)
            {
                LOG.severe(() -> ("ERROR: Could not obtain CICS Task"));
                return;
            }

            try
            {
                String userId = task.getUSERID();
                LOG.info("Task USERID = " + userId);
            }
            catch (InvalidRequestException e)
            {
                LOG.log(Level.FINE, "Failed to get userid", e);
            }

            LOG.info(() -> ("DEBUG: Topic = " + record.topic()));
            LOG.info(() -> ("DEBUG: Finished processing Kafka message in thread: " + Thread.currentThread().getName()
                + " " + record.value()));
        }


        @Override
        public String getTranid()
        {
            return config.getTranIdForTopic(record.topic());
        }
    }


}

