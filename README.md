# cics-java-liberty-springboot-kafka

[![Build](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/build.yaml/badge.svg)](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/build.yaml)
[![License](https://img.shields.io/badge/License-EPL%202.0-green.svg)](https://opensource.org/licenses/EPL-2.0)

## Overview

This sample demonstrates how to integrate Apache Kafka with IBM CICS using Spring Boot, deployed as a WAR file to a CICS Liberty JVM server on z/OS. The sample includes both Gradle and Maven build configurations for use in Eclipse or standalone build environments.

The sample is intended both as a runnable example and as an educational reference for developers learning to build enterprise-grade Kafka consumers.

**Key Features:**
- Consumes messages from multiple Kafka topics asynchronously
- Processes each message within a CICS transaction context
- Demonstrates security identity propagation in Liberty
- Shows how to map different topics to different CICS transaction IDs
- Provides two alternative security approaches for credential management

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Design and Architecture](#design-and-architecture)
4. [How It Works](#how-it-works)
5. [Security Models Explained](#security-models-explained)
6. [Before You Start: Files to Modify](#before-you-start-files-to-modify)
7. [Project Structure](#project-structure)
8. [Thread Pool Management and TCLASS Considerations](#thread-pool-management-and-tclass-considerations)
9. [Downloading](#downloading)
10. [Building the Sample](#building-the-sample)
11. [Deploying to a CICS Liberty JVM server](#deploying-to-a-cics-liberty-jvm-server)
12. [Running the Sample](#running-the-sample)
13. [Troubleshooting](#troubleshooting)
14. [Logging Strategy](#logging-strategy)
15. [License](#license)
16. [Additional Resources](#additional-resources)
17. [Contributing](#contributing)

---

## Design and Architecture

### High-Level Design Intent

This sample addresses a fundamental challenge: **how to consume Kafka messages within CICS transactions while maintaining security context**.

This sample has the following components:

1. **Uses Liberty's ManagedExecutorService** - Ensures threads are CICS-aware
2. **Explicit Security Context Propagation** - Captures and applies security identity.
3. **Per-Topic Transaction Mapping** - Routes messages to appropriate CICS transactions based on topic
4. **Controlled Lifecycle Management** - Allows dynamic start/stop of topic consumers via REST endpoints

### Architecture Diagram

**Alternative A: Subject-Based RunAs Identity (Default)**

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Cluster                           │
│                    (topics: orders, test-topic)                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Messages
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CICS Liberty JVM Server                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Spring Boot Application (WAR)                │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │         KafkaController (REST Endpoint)          │     │  │
│  │  │  • /control/start?topic=xxx                      │     │  │
│  │  │  • /control/stop?topic=xxx                       │     │  │
│  │  │  • Captures caller's Subject (HTTP auth)         │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │      KafkaConsumerService (Listeners)            │     │  │
│  │  │  • @KafkaListener per topic                      │     │  │
│  │  │  • Sets RunAs Subject on consumer thread         │     │  │
│  │  │  • Receives batches of messages                  │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │      KafkaMessageProcessor                       │     │  │
│  │  │  • Submits to ManagedExecutorService             │     │  │
│  │  │  • Wraps in CICSTransactionRunnable              │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │   Liberty ManagedExecutorService                 │     │  │
│  │  │  • CICS-aware thread pool                        │     │  │
│  │  │  • Inherits RunAs Subject                        │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │   CICSTransactionRunnable.run()                  │     │  │
│  │  │  • Executes under CICS transaction               │     │  │
│  │  │  • Transaction ID from KafkaBatchConfig          │     │  │
│  │  │  • Processes message business logic              │     │  │
│  │  └──────────────────────────────────────────────────┘     │  │
│  │                                                           │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Alternative B: authData-Based Identity with Programmatic Login**

The architecture is identical to Alternative A, with one key difference in the KafkaController:

```
┌──────────────────────────────────────────────────┐
│         KafkaController (REST Endpoint)          │
│  • /control/start?topic=xxx                      │
│  • /control/stop?topic=xxx                       │
│  • Uses LoginManager.getSubject()                │  ← Different from Alternative A
│    (programmatic login with authData)            │
└──────────────────────────────────────────────────┘
         │
         ▼
    (rest of flow identical to Alternative A)
```

**Key Difference:**
- **Alternative A:** Subject from authenticated HTTP request (`WSSubject.getCallerSubject()`)
- **Alternative B:** Subject from programmatic login (`LoginManager.getSubject()` using server.xml authData)
- **Both:** Use the same RunAs mechanism (`WSSubject.setRunAsSubject(subject)`)

See [Security Models Explained](#security-models-explained) for detailed comparison.

### Component Responsibilities

| Component | Purpose | Key Methods/Annotations |
|-----------|---------|------------------------|
| **KafkaApplication** | Spring Boot entry point | `@SpringBootApplication` |
| **KafkaController** | REST API for life-cycle control | `/start`, `/stop`, captures Subject |
| **KafkaConsumerService** | Per-topic Kafka listeners | `@KafkaListener`, sets RunAs Subject |
| **KafkaMessageProcessor** | Async message processing | Submits to ManagedExecutorService |
| **KafkaBatchConfig** | Topic-to-transaction mapping | `getTranIdForTopic()` |
| **LoginManager** | Alternative: Subject via programmatic login | JAAS login using authData (optional) |

---

## How It Works

### Message Flow (Step-by-Step)

1. **User Initiates Consumer**
   - HTTP GET/POST to `/control/start?topic=orders`
   - KafkaController obtains a Subject (security identity):
     - **Alternative A (default):** Captures caller's Subject from HTTP request
     - **Alternative B:** Uses LoginManager to get Subject from authData
   - Subject is stored in a map keyed by topic name
   - Spring Kafka listener container for that topic is started

2. **Kafka Messages Arrive**
   - Spring Kafka polls the broker and receives a batch of messages
   - Messages are delivered to the appropriate `@KafkaListener` method in KafkaConsumerService
   - Example: `onOrdersBatch()` for the "orders" topic

3. **Security Context is Applied**
   - The consumer thread calls `WSSubject.setRunAsSubject(subject)`
   - This establishes the security identity for all subsequent work
   - Liberty's ManagedExecutorService will inherit this identity

4. **Message Processing is Offloaded**
   - Each message in the batch is wrapped in a `CICSTransactionRunnable`
   - The runnable is submitted to Liberty's `ManagedExecutorService`
   - This ensures the processing happens on a CICS-aware thread

5. **CICS Transaction Executes**
   - The `CICSTransactionRunnable.run()` method executes
   - The transaction ID is determined by `getTranid()`, which looks up the topic in KafkaBatchConfig
   - Business logic processes the message (in this sample, just logging)

6. **User Stops Consumer**
   - HTTP GET/POST to `/control/stop?topic=orders`
   - Spring Kafka listener container is stopped
   - Subject mapping is removed from the map

### Multi-Topic Approach

This sample demonstrates **per-topic listeners with individual life-cycle control**:

```java
@KafkaListener(id = "ordersListener", topics = "orders", ...)
public void onOrdersBatch(List<ConsumerRecord<String, String>> batch) { ... }

@KafkaListener(id = "test-topicListener", topics = "test-topic", ...)
public void onTestBatch(List<ConsumerRecord<String, String>> batch) { ... }
```

**Why separate listeners?**
- Each topic can be started/stopped independently
- Different topics can use different CICS transaction IDs
- Allows per-topic security contexts (different users for different topics)
- Simplifies monitoring and troubleshooting

**Transaction ID Mapping:**
The `application.properties` file maps topics to transaction IDs:
```properties
cics.transaction.map.test-topic=KAFK
cics.transaction.map.orders=KAF1
```

If no mapping exists, the default transaction ID `CJSU` is used.

---

## Security Models Explained

This sample provides **two alternative approaches** for managing security credentials. Choose the one that best fits your operational requirements.

### Alternative A: Subject-Based RunAs Identity (Default - Implemented)

**How it works:**
1. User authenticates to Liberty
2. `/control/start` captures the caller's Subject
3. Consumer thread sets this Subject as its RunAs identity
4. ManagedExecutorService inherits the identity
5. CICS transactions run under this identity

**Configuration:**
- No special server.xml configuration needed
- Security is established via the HTTP request
- Each topic can have a different identity (different users call `/start`)

**Pros:**
- Simple configuration
- Flexible per-topic security

**Code Location:**
- `KafkaController.start()` - Captures Subject
- `KafkaConsumerService.handleBatch()` - Sets RunAs Subject

---

### Alternative B: authData-Based Identity with Programmatic Login (Alternative - Not Active by Default)

**How it works:**
1. Credentials are stored in Liberty's `server.xml` as `<authData>`
2. Password is AES-encrypted using a key from a RACF keyring
3. Application performs **programmatic JAAS login** using these credentials via `LoginManager`
4. Resulting **Subject** is obtained and used the same way as Alternative A
5. This Subject is then set as RunAs identity on consumer threads (same mechanism as Alternative A)

**Configuration Required:**

1. **Create the RACF key ring and certificates**

Run these TSO commands with appropriate IDs and DNs for your environment.
`YOUR.KEYRING` is the generic placeholder for your keyring name.

```tso
/* Create key ring (owned by Liberty STC user, e.g., <user_id>) */
RACDCERT ADDRING(YOUR.KEYRING) ID(<user_id>)

/* (Optional) Create a CERTAUTH root CA */
RACDCERT GENCERT CERTAUTH +
  SUBJECTSDN(CN('MyLibertyCA') C('UK') O('YourOrg') OU('Liberty')) +
  WITHLABEL('LIBERTY.CA') NOTAFTER(DATE(2030/12/31))

/* Create a personal certificate labeled 'Liberty' for <user_id> */
RACDCERT GENCERT ID(<user_id>) +
  SUBJECTSDN(CN('liberty.example.com') C('UK') O('YourOrg') OU('Liberty')) +
  WITHLABEL('Liberty') +
  SIGNWITH(CERTAUTH LABEL('LIBERTY.CA')) +
  RSA SIZE(2048) NOTAFTER(DATE(2028/12/31))

/* Connect CA + personal cert to the key ring */
RACDCERT ID(<user_id>) CONNECT(CERTAUTH LABEL('LIBERTY.CA') RING(YOUR.KEYRING))
RACDCERT ID(<user_id>) CONNECT(ID(<user_id>) LABEL('Liberty') RING(YOUR.KEYRING) USAGE(PERSONAL) DEFAULT)

/* Verify ring contents */
RACDCERT LISTRING(YOUR.KEYRING) ID(<user_id>)
```

**Expected output:**
```
Digital ring information for user <user_id>:

  Ring:
      >YOUR.KEYRING<
  Certificate Label Name             Cert Owner     USAGE      DEFAULT
  --------------------------------   ------------   --------   -------
  LIBERTY.CA                            CERTAUTH    CERTAUTH     NO
  Liberty                            ID(<user_id>)  PERSONAL     YES
```

**Reference:** [IBM Docs - JCERACFKS/JCECCARACFKS keyring setup](https://www.ibm.com/docs/en/was-liberty/nd?topic=ssl-configuring-keyring-based-keystore)

2. **Enable features in server.xml:**
```xml
<feature>passwordUtilities-1.0</feature>
<feature>zosPasswordEncryptionKey-1.0</feature>
```

3. **Configure RACF keyring in server.xml:**
```xml
<!-- Obtain AES key from RACF key ring at runtime -->
<zosPasswordEncryptionKey
    keyring="safkeyring:///YOUR.KEYRING"
    type="JCERACFKS"
    label="Liberty"/>

<!-- (Optional) Use RACF key ring as SSL keystore -->
<keyStore id="defaultKeyStore"
          fileBased="false"
          type="JCERACFKS"
          location="safkeyring:///YOUR.KEYRING"
          password="password"/>
```

4. **Generate AES-encoded password with `securityUtility` (key from RACF)**

From `${wlp.install.dir}/wlp/bin`, or from SSH shell on zFS:

```bash
securityUtility encode \
  --encoding=aes \
  --keyring=safkeyring:///YOUR.KEYRING \
  --keyringType=JCERACFKS \
  --keyLabel=Liberty \
  "YourRacfPassword"
```

This will output something like:
```
{aes}ARI673meZr9vyGHN8xKJdLx9...
```

5. **Define authData in server.xml:**
```xml
<!-- Credentials alias (AES-protected) -->
<authData id="cicsSAF" user="<user_id>" password="{aes}ARI673meZr9vy...."/>
```

6. **Activate LoginManager in code:**
Uncomment in `KafkaController.java`:
```java
@Autowired(required = false)
private LoginManager loginManager;
```

Then use in `start()` method:
```java
Subject subject = loginManager.getSubject();
```

**Pros:**
- Credentials managed centrally in server.xml
- No clear-text passwords in configuration
- Suitable for automated/service accounts
- Aligns with enterprise security policies

**Cons:**
- More complex setup (RACF keyring required)
- Requires z/OS security administrator involvement

**Code Location:**
- `LoginManager.java` - Performs programmatic login
- `KafkaController.start()` - Would use LoginManager instead of WSSubject.getCallerSubject()

---

## Prerequisites

### Workstation Requirements
* **Java:** Java 17 or later
* **Build Tools:**
  - **Gradle:** Recommended: 8.0+ - included via wrapper
  - **Maven:** Recommended: 3.9.0+ - included via wrapper
* **IDE (Optional):**
  - Eclipse with IBM CICS SDK for Java EE, Jakarta EE and Liberty
  - IntelliJ IDEA, VS Code, or any IDE with Gradle/Maven support
  - Command line (no IDE required if using wrappers)

### z/OS Requirements
* **CICS TS:** V6.3 or later
* **WebSphere Liberty:** Included with CICS
* **Java:** IBM Semeru Runtime 17 or later on z/OS

### Network Requirements
* Connectivity from z/OS to Kafka broker(s)
* HTTP/HTTPS access to Liberty server for REST API calls

---

## Before You Start: Files to Modify

Before building and deploying this sample, you **must** customize the following files with your environment-specific values:

### 1. Kafka Connection Configuration
**File:** `cics-java-liberty-springboot-kafka-app/src/main/resources/application.properties`

**What to change:**
```properties
# Replace with your Kafka broker address
spring.kafka.bootstrap-servers=<YOUR_KAFKA_BROKER_IP>:9092
# Optional: Change consumer group ID if needed
spring.kafka.consumer.group-id=<YOUR_CONSUMER_GROUP>
# Add topic-to-transaction mappings
cics.transaction.map.<your-topic>=<YOUR_TRANID>
```

**Example:**
```properties
spring.kafka.bootstrap-servers=9.109.246.51:9092
spring.kafka.consumer.group-id=my-consumer-group
cics.transaction.map.test-topic=KAFK
cics.transaction.map.orders=KAF1
```

---

### 2. Liberty Server Configuration
**File:** `etc/config/liberty/server.xml`

**For Alternative A (Subject-based - default):**
- Verify that the following entry has been included in the configuration after CICS Initialization (auto-added if SEC=YES in SIT)

```xml
<feature>cicsts:security-1.0</feature>
```
- Verify application location matches deployment path

**For Alternative B (authData-based):**
- See detailed setup instructions in [Security Models Explained - Alternative B](#alternative-b-authdata-based-identity-with-programmatic-login-alternative-not-active-by-default)
- Requires: RACF keyring, AES-encrypted password, authData configuration

---

### 3. Build Configuration (Optional)
**Files:**
- `cics-java-liberty-springboot-kafka-cicsbundle/build.gradle`
- `cics-java-liberty-springboot-kafka-cicsbundle/pom.xml`

**When is this needed?**
Only if using **Method 1: CICS Bundle Deployment** (see [Deploying to a CICS Liberty JVM server](#deploying-to-a-cics-liberty-jvm-server)). This tells the CICS bundle plugins which Liberty JVM server will run your application.

**What to change:**
```gradle
// Gradle: Set your target JVM server name
cics.jvmserver = '<YOUR_JVMSERVER_NAME>'  // e.g., 'DFHWLP'
```

```xml
<!-- Maven: Set your target JVM server name -->
<defaultjvmserver><YOUR_JVMSERVER_NAME></defaultjvmserver>  <!-- e.g., DFHWLP -->
```

**Note:** If using Method 2 (CICS Explorer) or Method 3 (Direct Liberty), you can skip this step.

---

### 4. Java Code (Only for Alternative B)

**Required changes:**
- Uncomment `LoginManager` in `KafkaController.java`
- Update `AUTH_DATA_ID` in `LoginManager.java` to match your server.xml

**See detailed instructions in:** [Security Models Explained - Alternative B, Step 6](#alternative-b-authdata-based-identity-with-programmatic-login-alternative-not-active-by-default)

---

### Summary Checklist

Before building:
- [ ] Updated `application.properties` with Kafka broker address
- [ ] Configured topic-to-transaction mappings in `application.properties`
- [ ] Chose security Alternative (A or B)
- [ ] If Alternative A: Updated `server.xml` with features
- [ ] If Alternative B: Created RACF keyring and generated AES password
- [ ] If Alternative B: Updated `server.xml` with features, keyring and authData
- [ ] If Alternative B: Uncommented LoginManager in `KafkaController.java`
- [ ] Updated JVM server name in build files (if using CICS bundle deployment)

---

## Project Structure

```
cics-java-liberty-springboot-kafka/
├── README.md                                            # This file
├── LICENSE                                              # EPL 2.0 license
├── build.gradle                                         # Root Gradle build
├── settings.gradle                                      # Gradle multi-project settings
├── pom.xml                                              # Root Maven POM
├── gradlew / gradlew.bat                                # Gradle wrapper scripts
├── mvnw / mvnw.cmd                                      # Maven wrapper scripts
│
├── cics-java-liberty-springboot-kafka-app/              # Main application
│   ├── build.gradle                                     # App-level Gradle build
│   ├── pom.xml                                          # App-level Maven POM
│   └── src/main/
│       ├── java/com/ibm/cicsdev/springboot/kafka
│       │   ├── KafkaApplication.java                    # Spring Boot entry point
│       │   ├── KafkaController.java                     # REST API for start/stop
│       │   ├── KafkaConsumerService.java                # Kafka listeners per topic
│       │   ├── KafkaMessageProcessor.java               # Async message processing
│       │   ├── KafkaBatchConfig.java                    # Topic-to-transaction mapping
│       │   ├── LoginManager.java                        # Optional: authData login
│       │   └── ServletInitializer.java                  # WAR deployment support
│       ├── resources/
│       │   └── application.properties                   # Kafka & Spring config
│       └── webapp/WEB-INF/
│           └── web.xml                                  # Web app descriptor
│
├── cics-java-liberty-springboot-kafka-cicsbundle/       # CICS bundle (Gradle/Maven)
│   ├── build.gradle                                     # Bundle Gradle build
│   ├── pom.xml                                          # Bundle Maven POM
│   └── src/main/bundleParts/
│       └── KAFK.transaction                             # CICS transaction definition
│
├── cics-java-liberty-springboot-kafka-cicsbundle-eclipse/  # Eclipse CICS bundle project
│   ├── META-INF/cics.xml                                # Bundle manifest
│   └── cics-java-liberty-springboot-kafka.warbundle     # WAR bundle part descriptor
│
└── etc/config/liberty/
    └── server.xml                                       # Liberty server template
```

---

## Thread Pool Management and TCLASS Considerations

### Overview

In CICS Liberty environments with multiple applications sharing a single JVM server, proper thread pool management is critical to prevent application starvation and deadlocks. This section explains the challenges and solutions implemented in this sample.

---

### The Problem: Thread Starvation in Multi-Application Environments

**Architecture Context:**
- Multiple web applications run inside a single CICS Liberty JVM server
- Each application may have its own endpoint/virtualhost
- CICS regions are cloned for scalability/resilience
- Sysplex distributor load-balances between applications across cloned regions

**The Challenge:**

In traditional CICS, you use **TCLASS** to throttle applications and prevent any one application from consuming too much resource. However, in Liberty, this approach can cause **deadlocks**:

1. **HTTP requests enter Liberty** and are allocated a thread from the shared pool
2. **Only then** does the CICS intercepter check TCLASS limits
3. **If TCLASS limit is reached**, the requesting thread is blocked
4. **The blocked thread is NOT released** back to the thread pool
5. **Result**: The entire server can deadlock, starving other applications

**Why This Matters:**

- Liberty in CICS has a **maximum thread limit of 256**
- Application starvation can occur at lower request rates than expected
- One misbehaving application can impact all applications in the JVM server

---

### The Solution: Custom Managed Executors

This sample implements **custom ManagedExecutorService** with application-specific thread limits to prevent starvation.

#### How It Works

Instead of using Liberty's default executor (shared by all applications), each application defines its own executor with specific concurrency limits:

```xml
<!-- In server.xml -->
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="10"
     coreThreads="5"/>
```

**Benefits:**
- ✅ Kafka application cannot consume more than 'maxThreads'
- ✅ Other applications in the same JVM server are protected
- ✅ Prevents deadlock scenarios with TCLASS
- ✅ Provides predictable resource allocation

#### Implementation in Code

The `KafkaMessageProcessor` injects the custom executor:

```java
@Resource(lookup = "concurrent/KafkaExecutor")
private ManagedExecutorService executor;
```

When Kafka messages arrive, they are submitted to this bounded executor:

```java
executor.submit(new KafkaCICSTransactionRunnable(record, config));
```

If the executor's queue is full, new submissions will block or be rejected (depending on policy), preventing unbounded thread consumption.

---

### Configuration Guidelines

#### Determining Thread Limits

Consider these factors when setting `maxThreads`:

1. **Expected Message Rate**: How many messages per second?
2. **Processing Time**: How long does each message take to process?
3. **Other Applications**: How many other apps share this JVM server?
4. **Total Thread Budget**: CICS Liberty max is 256 threads

#### Recommended Configuration

**For Low-Volume Applications (< 100 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="10"
     coreThreads="3"/>
```

**For Medium-Volume Applications (100-500 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="20"
     coreThreads="5"/>
```

**For High-Volume Applications (> 500 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="40"
     coreThreads="10"/>
```

---

## Downloading

**If using Eclipse:** the simplest approach is to clone the repository using the Eclipse Git plugin (EGit) perspective.

**If using the command line:**
```shell
git clone https://github.com/cicsdev/cics-java-liberty-springboot-kafka
```
Alternatively, download the sample as a [ZIP](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/archive/main.zip) and unzip onto the workstation.

**If importing into Eclipse:**
1. In the **Git Repositories** view, right-click the repository → **Import as Project** (imports the root project)
   *(if you cloned from the command line, use **File → Import → Existing Projects into Workspace** instead, browse to the cloned directory, select all projects, and skip to step 6)*
2. Switch to the **Java EE** perspective
3. In the **Project Explorer**, right-click the `cics-java-liberty-springboot-kafka-app` folder → **Import as Project**
4. Right-click the `cics-java-liberty-springboot-kafka-cicsbundle` folder → **Import as Project**
5. Right-click the `cics-java-liberty-springboot-kafka-cicsbundle-eclipse` folder → **Import as Project**
6. **Required:** Right-click the root project → **Gradle → Refresh Gradle Project** or **Maven → Update Project...** — this resolves Spring Boot and CICS dependencies into the project classpath. Without this step, the WTP export will produce an incomplete WAR missing `WEB-INF/lib`.

### Check dependencies

Before building this sample, verify that the correct CICS TS bill of materials (BOM) is specified for your target release of CICS. The BOM specifies a consistent set of artifacts, and adds information about their scope. You can browse the published versions of the CICS BOM at [Maven Central](https://mvnrepository.com/artifact/com.ibm.cics/com.ibm.cics.ts.bom).

Gradle (`cics-java-liberty-springboot-kafka-app/build.gradle`):

`compileOnly(enforcedPlatform("com.ibm.cics:com.ibm.cics.ts.bom:6.3-20250905155520"))`

Maven (`cics-java-liberty-springboot-kafka-app/pom.xml`):

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.ibm.cics</groupId>
      <artifactId>com.ibm.cics.ts.bom</artifactId>
      <version>6.3-20250905155520</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

---

## Building the Sample

You can build using Gradle, Maven, or Eclipse. The wrappers are pre-configured with compatible versions.

### Gradle Wrapper (command line)

On Linux or Mac:

```shell
./gradlew clean build
```

On Windows:

```shell
gradlew.bat clean build
```

This creates a WAR file inside the `cics-java-liberty-springboot-kafka-app/build/libs` directory and a CICS bundle ZIP inside `cics-java-liberty-springboot-kafka-cicsbundle/build/distributions`.

> **Note:** In Eclipse, the `build` directory may be hidden by default. To view it: **Package Explorer → ⋮ → Filters and Customization → uncheck "Gradle build folder"**. For Maven, the `target` directory is visible by default.

### Maven Wrapper (command line)

On Linux or Mac:

```shell
./mvnw clean verify
```

On Windows:

```shell
mvnw.cmd clean verify
```

This creates a WAR file inside the `cics-java-liberty-springboot-kafka-app/target` directory and a CICS bundle ZIP inside `cics-java-liberty-springboot-kafka-cicsbundle/target`.

---

## Deploying to a CICS Liberty JVM server

### CICS Bundle Plugin Deployment (Gradle/Maven)

1. **Upload the bundle ZIP to zFS:**
   ```bash
   # From your workstation (using secure copy)
   scp cics-java-liberty-springboot-kafka-cicsbundle/build/distributions/*.zip user@zos:/path/to/bundles/
   ```
   
   **Note:** `scp` is a standard Unix/Linux command for secure file transfer. Replace `user@zos` with your z/OS credentials and `/path/to/bundles/` with your target directory.

2. **Extract on z/OS:**
   ```bash
   # On z/OS
   cd /path/to/bundles
   jar xf cics-java-liberty-springboot-kafka-cicsbundle-1.0.0.zip
   ```

3. **Define and install the bundle:**
   See [Common Bundle Installation Steps](#common-bundle-installation-steps) below.

---

### CICS Explorer SDK Deployment

This method uses IBM CICS Explorer (an Eclipse-based IDE) to create a CICS bundle and deploy it directly to z/OS. This approach is ideal for developers who prefer a GUI-based deployment workflow and want integrated tooling for CICS development.

**Prerequisites:**
- IBM CICS Explorer installed on your workstation
- CICS Explorer configured with connection to your z/OS system
- SSH/SFTP access to z/OS UNIX System Services (USS)
- CICS region configured and running

#### Step 1: Review CICS Bundle Project in Eclipse

A CICS bundle is a deployment package that can contain multiple resources (WARs, JARs, OSGi bundles, etc.) and their metadata.
The Bundle Project is already provided in the repository (imported when you cloned the repo).

1. **Locate the Bundle Project:**
    In Project Explorer, locate `cics-java-liberty-springboot-kafka-cicsbundle-eclipse` (imported when you cloned the repository).

    Look into the project to verify its structure and contents.

2. **Verify WAR Bundle Part Configuration:**
    - Locate the `cics-java-liberty-springboot-kafka.warbundle` file
    - Confirm that the JVM server is correctly specified (e.g., `DFHWLP`)

3. **Review Bundle Files:**

   The bundle project contains:
   ```
   cics-java-liberty-springboot-kafka-cicsbundle-eclipse/
   ├── META-INF/
   │   └── cics.xml                                     # Bundle manifest
   └── cics-java-liberty-springboot-kafka.warbundle     # WAR bundle part descriptor
   ```

   **cics.xml:**
   ```xml
   <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
   <manifest xmlns="http://www.ibm.com/xmlns/prod/cics/bundle" id="cics-java-liberty-springboot-kafka-bundle"
       bundleMajorVer="1" bundleMinorVer="0" bundleMicroVer="0" bundleVersion="1" bundleRelease="0">
       <define name="cics-java-liberty-springboot-kafka-app"
           type="http://www.ibm.com/xmlns/prod/cics/bundle/WARBUNDLE"
           path="cics-java-liberty-springboot-kafka.warbundle"/>
   </manifest>
   ```
   - `id`: Unique identifier for this bundle in CICS (`cics-java-liberty-springboot-kafka-bundle`)
   - `define`: References the WAR bundle part

   **.warbundle file:**
   ```xml
   <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
   <warbundle symbolicname="cics-java-liberty-springboot-kafka-app" jvmserver="DFHWLP"/>
   ```
   - `symbolicname`: Must match the Eclipse project name of the `-app` project (`cics-java-liberty-springboot-kafka-app`) so WTP can locate the WAR for export
   - `jvmserver`: Target Liberty JVM server name

#### Step 2: Export Bundle to z/OS UNIX File System (zFS)

This step deploys your bundle to z/OS and makes it available to CICS.

1. **Initiate Export:**
   - In **Project Explorer**, right-click on your Bundle Project
   - Select **Export Bundle Project to z/OS UNIX File System**
   - Click **Next**

2. **Specify Bundle Deployment Location:**
   
   Choose where on z/OS to deploy the bundle:
   - **Target directory**: `/u/cicsts/bundles/cics-java-liberty-springboot-kafka-cicsbundle_1.0.0`
   
   **Important Path Considerations:**
   - The directory will be created if it doesn't exist

   Click **Finish** to start the export.

   Ensure that the CICS region user has read and write access to the bundle directory structure.

#### Step 3: Define and Install the Bundle in CICS

After export, define and install the bundle in your CICS region. See [Common Bundle Installation Steps](#common-bundle-installation-steps) below.

#### Step 4: Verify Deployment

1. **Check Liberty Messages:**
   - View Liberty server logs
   - Look for:
   ```
   [AUDIT   ] CWWKT0016I: Web application available (default_host):
              http://hostname:9080/cics-java-liberty-springboot-kafka/
   ```

2. **Verify in CICS Explorer:**
   - Navigate to **Bundle Definitions** → `KFKABNDL`
   - Status should show: **Enabled** and **Installed**

---

### Direct Liberty Application Deployment

1. **Upload WAR to zFS:**
   ```bash
   # From your workstation (using secure copy)
   scp cics-java-liberty-springboot-kafka-app/build/libs/*.war user@zos:/path/to/liberty/apps/
   ```
   
   **Note:** Replace `user@zos` with your z/OS credentials and `/path/to/liberty/apps/` with your Liberty apps directory.

2. **Add to server.xml:**
   ```xml
   <application id="cics-java-liberty-springboot-kafka"
       location="${server.config.dir}/apps/cics-java-liberty-springboot-kafka.war"
       name="cics-java-liberty-springboot-kafka" type="war">
       <application-bnd>
           <security-role name="cicsAllAuthenticated">
               <special-subject type="ALL_AUTHENTICATED_USERS"/>
           </security-role>
       </application-bnd>
   </application>
   ```

3. **Restart Liberty or refresh configuration**

---

### Common Bundle Installation Steps

These steps apply to both Method 1 and Method 2 after the bundle is on z/OS.

**Option A: Using CEDA Commands**

1. **Define the Bundle:**
   ```
   CEDA DEFINE BUNDLE(KFKABNDL)
        GROUP(MYGROUP)
        BUNDLEDIR(/u/cicsts/bundles/cics-java-liberty-springboot-kafka-cicsbundle-1.0.0)
        STATUS(ENABLED)
   ```

2. **Install the Bundle:**
   ```
   CEDA INSTALL BUNDLE(KFKABNDL) GROUP(MYGROUP)
   ```

3. **Enable (if needed):**
   ```
   CEDA SET BUNDLE(KFKABNDL) ENABLED
   ```

**Option B: Using CICS Explorer UI**

1. Navigate to **CICS SM** (Systems Management) view
2. Expand your CICS region → **Bundle Definitions**
3. Right-click → **New** → **Bundle Definition**
4. Fill in:
   - Name: `KFKABNDL`
   - Group: `MYGROUP`
   - Bundle Directory: `/u/cicsts/bundles/cics-java-liberty-springboot-kafka-cicsbundle-1.0.0`
   - Status: `Enabled`
5. Save and right-click → **Install**

**Verify Installation:**
- Check Liberty messages.log for application startup
- In CICS Explorer: Bundle Definitions → `KFKABNDL` should show **Enabled** and **Installed**

---

## Running the Sample

### Step 1: Verify Deployment

Check Liberty messages.log for successful application start:
```
[AUDIT   ] CWWKT0016I: Web application available (default_host): http://hostname:9080/cics-java-liberty-springboot-kafka/
```

---

### Step 2: Start a Kafka Consumer

**Alternative A (Subject-Based - Default):**

Using curl with authentication:
```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/start?topic=test-topic" \
     -u username:password
```

Using a browser (will prompt for credentials):
```
http://hostname:9080/cics-java-liberty-springboot-kafka/control/start?topic=test-topic
```

**Alternative B (authData-Based):**

Using curl (no authentication required - uses programmatic login):
```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/start?topic=test-topic"
```

**Expected Response (both alternatives):**
```
Started listener for topic=test-topic
```

---

### Step 3: Verify Message Processing

Check Liberty messages.log:
```
[INFO] Received message from topic test-topic: Hello from Kafka!
[INFO] Task USERID = TESTUSER
[INFO] DEBUG: Topic = test-topic
[INFO] DEBUG: Finished processing Kafka message in thread: Default Executor-thread-1 Hello from Kafka!
```

---

### Step 4: Stop the Consumer

**Alternative A (Subject-Based - Default):**
```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/stop?topic=test-topic" \
     -u username:password
```

**Alternative B (authData-Based):**
```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/stop?topic=test-topic"
```

**Expected Response (both alternatives):**
```
Stopped listener for topic=test-topic
```

---

### Multiple Topics

You can run multiple consumers simultaneously. Each topic runs independently with its own transaction ID:

Start consumer for orders topic:
```bash
curl -X POST ".../control/start?topic=orders" -u username:pass
```

Start consumer for test-topic:
```bash
curl -X POST ".../control/start?topic=test-topic" -u username:pass
```

---

## Troubleshooting

### Issue: "Failed to set RunAsSubject"

**Symptom:** WSSecurityException in logs

**Cause:** Subject is null or invalid

**Solution:**
- Verify user is authenticated when calling `/start`
- Check Liberty security configuration
- For Alternative B: Verify authData is configured correctly

---

### Issue: AES password decryption fails (Alternative B)

**Symptom:** "Failed to decrypt password" or login failure

**Cause:** RACF keyring or AES key misconfigured

**Solution:**
1. Verify keyring exists: `RACDCERT LISTRING(<keyring>) ID(<userid>)`
2. Verify certificate label matches: `label="Liberty"`
3. Regenerate AES password with correct keyring path
4. Ensure Liberty user has access to keyring

---

### Logging and Diagnostics

**Enable detailed logging in server.xml:**
```xml
<logging traceSpecification="*=info:com.ibm.cicsdev.springboot.kafka.*=all"/>
```

**Key log locations:**
- Liberty: `${wlp.user.dir}/servers/<server>/logs/messages.log`
- CICS: MSGUSR, CSSL transient data queues
- Kafka: Check broker logs if messages aren't being produced

---

## Logging Strategy

This sample uses **Java Util Logging (JUL)** for simplicity and integration with Liberty.

**Why JUL?**
- Built into JDK (no dependencies)
- Integrates with Liberty's logging infrastructure
- Thread-safe and efficient

**Why not System.out.println?**
- Not thread-safe
- Poor performance under concurrency
- Bypasses Liberty logging configuration

**Why not Log4j/SLF4J?**
- Adds unnecessary dependencies for a sample
- Introduces classloader complexity
- JUL is sufficient for this use case

**Viewing logs:**
By default, JUL output appears in `messages.log`. Ensure this JVM option is set:
```
-Dcom.ibm.ws.logging.console.log.level=INFO
```

---

## License

This project is licensed under [Eclipse Public License - v 2.0](LICENSE).

By downloading, installing, and/or using this sample, you acknowledge that separate license terms may apply to any dependencies required for installation, execution, or automated builds, including IBM CICS development components: https://www.ibm.com/support/customer/csol/terms/?id=L-ACRR-BBZLGX

---

## Additional Resources

- [CICS Liberty Documentation](https://www.ibm.com/docs/en/cics-ts/latest?topic=liberty-cics)
- [Spring Boot Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

---

## Contributing

This sample is maintained by IBM CICS development. We welcome bug reports and feature requests via GitHub Issues. Contributions are welcome and reviewed on a case-by-case basis — please read the [contributing guidelines](https://github.com/cicsdev/.github/blob/main/CONTRIBUTING.md) before opening a pull request. For CICS product questions, contact IBM Support.
