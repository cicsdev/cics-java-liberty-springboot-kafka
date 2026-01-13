# cics-java-liberty-springboot-kafka
[![Build](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml/badge.svg)](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml)

This project demonstrates a Spring Boot–based Kafka consumer integrated with IBM CICS and deployed as a WAR to a CICS Liberty JVM server on z/OS. The application processes Kafka messages asynchronously under the caller’s security context and avoids clear-text credentials by storing them as AES-encrypted values in Liberty server.xml, with the AES key held in a RACF key ring. The sample includes both Gradle and Maven build configurations for use in Eclipse or standalone build environments.

---

## Requirements
* Java 17 or later on the workstation
   For Java 17+ support, ensure you have:

    * **Gradle**: Version 7.3 or later (recommended: 8.0+)
      - Gradle 7.3+ is required for Java 17 support
      - Gradle 8.x provides better Java 17-21 compatibility
      
    * **Maven**: Version 3.8.1 or later (recommended: 3.9.0+)
      - Maven 3.8.1+ is required for Java 17 support
      - Maven 3.9.x provides improved performance and Java 17+ compatibility

    **Note**: The included Gradle and Maven wrapper scripts are pre-configured with compatible versions.
* WebSphere Liberty
* One of the following on your workstation:
  Eclipse with the IBM CICS SDK for Java EE, Jakarta EE and Liberty
  An IDE of your choice that supports Gradle or Maven (or can run the Wrappers)
  A command line, to run the Wrappers or to invoke a locally installed version of Gradle or Maven

---