
package com.ibm.cicsdev.springboot.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.websphere.security.WSSecurityException;
import com.ibm.websphere.security.auth.WSSubject;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;


@DeclareRoles({ "cics-user" })
@RolesAllowed("cics-user")
@RestController
@RequestMapping("/control")
public class KafkaController
{

    private static final Logger LOG = Logger.getLogger(KafkaController.class.getName());

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    // Present in app but inactive; uncomment in start() to enable programmatic login
    // @Autowired(required = false)
    // private LoginManager loginManager;

    // Per-topic Subject captured at START
    private final Map<String, Subject> topicSubjects = new ConcurrentHashMap<>();


    /**
     * Start consumption for a single topic under the caller's Liberty Subject (JWT/OIDC/Basic).
     *
     * <p>
     * The Subject represents the authenticated user who called /start. This identity will be used for
     * all CICS transactions processing messages from this topic. Different topics can run under different identities
     * (different users call /start).
     *
     * @param topic
     *            Name of the Kafka topic to start consuming
     * @return Response indicating success or failure
     */
    @RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
    public ResponseEntity<String> start(@RequestParam String topic)
        throws Exception
    {

        // Validate 'topic' early to avoid NPE in the map.
        if (topic == null || topic.isBlank())
        {
            return ResponseEntity.badRequest().body("ERROR: missing required query parameter 'topic'");
        }

        // 1. Capture the authenticated user's Subject
        Subject subject = null;
        try
        {
            subject = WSSubject.getCallerSubject();
            LOG.info("DEBUG: Subject is: " + subject);
        }
        catch (WSSecurityException e)
        {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body("ERROR: cannot obtain caller subject: " + e.getMessage());
        }

        // Verify authentication
        if (subject == null)
        {
            ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("ERROR: unauthenticated request");
        }

        // 2. Store it for this topic
        topicSubjects.put(topic, subject);

        // Calculate the ID of the KafkaListener container
        String listenerId = listenerIdFor(topic);
        var container = registry.getListenerContainer(listenerId);

        // Did we find a match?
        if (container == null)
        {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No listener found with id=" + listenerId);
        }

        // If it's not already running, start it
        if (!container.isRunning())
        {
            // start only this listener by id
            container.start();
            return ResponseEntity.ok("Started listener for topic=" + topic);
        }

        return ResponseEntity.ok("Listener already running for topic=" + topic);
    }


    /**
     * Deactivate a Kafka topic. Spring Kafka stops polling and finishes the current batch before the thread exits.
     * 
     * @param topic
     *            Kafka topic to stop
     * @return HTTP Response indicating success or failure
     */
    @RequestMapping(value = "/stop", method = { RequestMethod.POST, RequestMethod.GET })
    public ResponseEntity<String> stop(@RequestParam String topic)
    {
        if (topic == null || topic.isBlank())
        {
            return ResponseEntity.badRequest().body("ERROR: missing required query parameter 'topic'");
        }

        // Find the right Listener
        String listenerId = listenerIdFor(topic);
        var container = registry.getListenerContainer(listenerId);
        if (container == null)
        {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No listener found with id=" + listenerId);
        }

        // If it's running, stop it
        if (container.isRunning())
        {
            container.stop();
        }

        // and remove the topic-subject mapping
        topicSubjects.remove(topic);
        LOG.info(() -> ("Stopped listener for topic " + topic));
        return ResponseEntity.ok("Stopped listener for topic=" + topic);
    }


    static String listenerIdFor(String topic)
    {
        return topic + "Listener";
    }


    // Expose for injection in listeners
    Map<String, Subject> getTopicSubjects()
    {
        return topicSubjects;
    }
}
