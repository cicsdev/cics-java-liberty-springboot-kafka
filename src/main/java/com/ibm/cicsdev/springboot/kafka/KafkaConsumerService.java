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

import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.ibm.websphere.security.WSSecurityException;
import com.ibm.websphere.security.auth.WSSubject;


/**
 * KafkaConsumerService manages **per-topic Kafka consumers** in Liberty.
 *
 * <p>
 * Features: <br>
 * - Sets Liberty RunAs Subject to ensure proper CICS transaction identity. <br>
 * - Handles incoming messages asynchronously via KafkaMessageProcessor.
 * </p>
 */
@Service
public class KafkaConsumerService
{

    private static final Logger LOG = Logger.getLogger(KafkaConsumerService.class.getName());

    private final KafkaMessageProcessor processor;
    private final KafkaController control;

    // Guard: one-time RunAs initialisation per consumer thread
    private final ThreadLocal<Boolean> runAsInitialized = ThreadLocal.withInitial(() -> false);
    private final ThreadLocal<Subject> previousRunAs = new ThreadLocal<>();


    public KafkaConsumerService(KafkaMessageProcessor processor, KafkaController control)
    {
        this.processor = processor;
        this.control = control;
    }


    // --- Per-topic batch listeners (distinct IDs for selective START/STOP) ---
    @KafkaListener(id = "ordersListener", topics = "orders", groupId = "test-group", containerFactory = "batchFactory",
        autoStartup = "false")
    public void onOrdersBatch(List<ConsumerRecord<String, String>> batch)
    {
        handleBatch("orders", batch);
    }

    @KafkaListener(id = "test-topicListener", topics = "test-topic", groupId = "test-group",
        containerFactory = "batchFactory", autoStartup = "false")
    public void onTestBatch(List<ConsumerRecord<String, String>> batch)
    {
        handleBatch("test-topic", batch);
    }


    /**
     * Common batch handler:
     * 
     * <p>
     * - Gets per-topic Subject captured at /control/start <br>
     * - Sets RunAs ONCE per consumer thread(first batch) with safe exception handling <br>
     * - Submits each record to the existing async processor
     */
    private void handleBatch(String topic, List<ConsumerRecord<String, String>> batch)
    {
        // Get the Subject for this topic
        Subject subject = control.getTopicSubjects().get(topic);
        if (subject == null)
        {
            LOG.warning(() -> "No Subject for topic '" + topic
                + "' — skipping batch (start was not called or identity missing).");
            return;
        }

        // Set RunAs on the listener thread ONCE; Liberty executor captures this identity
        if (!runAsInitialized.get())
        {
            try
            {
                // Cache previous RunAs for optional restore (rarely needed when threads exit on STOP)
                Subject prev = WSSubject.getRunAsSubject();
                previousRunAs.set(prev);

                WSSubject.setRunAsSubject(subject);
                runAsInitialized.set(true);

                LOG.fine(() -> "RunAs set for topic '" + topic + "' on consumer thread.");
            }
            catch (WSSecurityException e)
            {
                LOG.log(Level.SEVERE, "Failed to set RunAsSubject for topic '" + topic + "': " + e.getMessage(), e);
                return;
            }
        }

        // Offload each record to the message processor (which should submit to Liberty MES)
        for (ConsumerRecord<String, String> rec : batch)
        {
            processor.processAsynchronous(rec);
        }
    }


    /**
     * Optional explicit restore (typically not required because consumer threads exit on STOP).
     */
    public void restoreRunAsIfInitialized()
    {
        if (runAsInitialized.get())
        {
            try
            {
                Subject prev = previousRunAs.get();
                if (prev != null)
                {
                    WSSubject.setRunAsSubject(prev);
                }
            }
            catch (WSSecurityException e)
            {
                LOG.log(Level.WARNING, "Failed to restore previous RunAsSubject: " + e.getMessage(), e);
            }
            finally
            {
                runAsInitialized.remove();
                previousRunAs.remove();
            }
        }
    }


    // If you ever need it, here's a safe wrapper to obtain the caller subject.
    @SuppressWarnings("unused")
    private Optional<Subject> safeGetCallerSubject()
    {
        try
        {
            return Optional.ofNullable(WSSubject.getCallerSubject());
        }
        catch (WSSecurityException e)
        {
            LOG.log(Level.WARNING, "Failed to get caller Subject: " + e.getMessage(), e);
            return Optional.empty();
        }
    }
}
