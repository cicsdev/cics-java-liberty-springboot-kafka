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
 * <b>Key Responsibilities:</b>
 * <ul>
 * <li>Submits messages to Liberty's ManagedExecutorService for async processing</li>
 * <li>Wraps each message in a CICSTransactionRunnable for CICS transaction context</li>
 * <li>Maps topics to CICS transaction IDs dynamically</li>
 * <li>Ensures messages run on CICS-aware threads</li>
 * <li>Uses custom executor with thread limits to prevent application starvation</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Thread Pool Management:</b><br>
 * This class uses a custom ManagedExecutorService (concurrent/KafkaExecutor) instead of
 * the default executor. This prevents the Kafka application from consuming all available
 * threads in the Liberty thread pool, which could starve other applications in the same
 * CICS Liberty JVM server.
 * </p>
 *
 * <p>
 * The custom executor is configured in server.xml with specific thread limits:
 * <ul>
 * <li>maxThreads: Maximum concurrent message processing threads</li>
 * <li>coreThreads: Minimum threads kept alive</li>
 * </ul>
 * This configuration is especially important in CICS environments where TCLASS limits
 * might cause thread blocking, and prevents deadlock scenarios.
 * </p>
 */
@Service
public class KafkaMessageProcessor
{
    private static final Logger LOG = Logger.getLogger(KafkaMessageProcessor.class.getName());

    /**
     * Custom ManagedExecutorService for processing Kafka messages asynchronously.
     *
     * <p>
     * <b>Alternative:</b> To use the default Liberty executor instead, comment out this line
     * and uncomment the following:
     * <pre>
     * &#64;Resource(lookup = "java:comp/DefaultManagedExecutorService")
     */
    @Resource(lookup = "concurrent/KafkaExecutor")
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
     * <p>
     * ManagedExecutorService is provided by Liberty. It creates threads that are CICS-aware (can
     * call Task.getTask()). CICSTransactionRunnable ensures work runs in a CICS transaction. The transaction ID is
     * determined dynamically based on the topic.
     *
     * @param record
     */
    public void processAsynchronous(ConsumerRecord<String, String> record)
    {
        LOG.info(() -> "Received message from topic " + record.topic() + ": " + record.value());
        executor.submit(new KafkaCICSTransactionRunnable(record, config));
    }


    /**
     * Runnable wrapper that executes a Kafka message within a CICS transaction.
     *
     * <p>
     * This runs on a CICS-aware thread created by Liberty's ManagedExecutorService.
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
            // Map topic to transaction ID dynamically
            return config.getTranIdForTopic(record.topic());
        }
    }


}

