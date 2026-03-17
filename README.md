# cics-java-liberty-springboot-kafka

[![Build](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml/badge.svg)](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml)

## Overview

This sample demonstrates how to integrate Apache Kafka with IBM CICS using Spring Boot, deployed as a WAR file to a CICS Liberty JVM server on z/OS. The sample includes both Gradle and Maven build configurations for use in Eclipse or standalone build environments.

The sample follows CICSDev best practices and is intended both as a runnable example and as an educational reference for developers learning to build enterprise-grade Kafka consumers.

**What This Sample Does:**
- Consumes messages from multiple Kafka topics asynchronously
- Processes each message within a CICS transaction context
- Demonstrates proper security identity propagation in Liberty
- Shows how to map different topics to different CICS transaction IDs
- Provides two alternative security approaches for credential management

---

## Table of Contents

1. [Design and Architecture](#design-and-architecture)
2. [How It Works](#how-it-works)
3. [Security Models Explained](#security-models-explained)
4. [Before You Start: Files to Modify](#before-you-start-files-to-modify)
5. [Requirements](#requirements)
6. [Project Structure](#project-structure)
7. [Configuration Guide](#configuration-guide)
8. [Building the Sample](#building-the-sample)
9. [Deploying to CICS](#deploying-to-cics)
10. [Running the Sample](#running-the-sample)
11. [Understanding the Code](#understanding-the-code)
12. [Troubleshooting](#troubleshooting)
13. [License](#license)

---

## Design and Architecture

### High-Level Design Intent

This sample addresses a fundamental challenge: **how to safely consume Kafka messages within CICS transactions while maintaining proper security context**.

This sample has the following components:

1. **Uses Liberty's ManagedExecutorService** - Ensures threads are CICS-aware
2. **Explicit Security Context Propagation** - Captures and applies security identity.
3. **Per-Topic Transaction Mapping** - Routes messages to appropriate CICS transactions based on topic
4. **Controlled Lifecycle Management** - Allows dynamic start/stop of topic consumers via REST endpoints

### Architecture Diagram

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
│  │  │  • Captures caller's Subject                     │     │  │
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

### Component Responsibilities

| Component | Purpose | Key Methods/Annotations |
|-----------|---------|------------------------|
| **KafkaApplication** | Spring Boot entry point | `@SpringBootApplication` |
| **KafkaController** | REST API for lifecycle control | `/start`, `/stop`, captures Subject |
| **KafkaConsumerService** | Per-topic Kafka listeners | `@KafkaListener`, sets RunAs Subject |
| **KafkaMessageProcessor** | Async message processing | Submits to ManagedExecutorService |
| **KafkaBatchConfig** | Topic-to-transaction mapping | `getTranIdForTopic()` |
| **LoginManager** | Alternative: Subject via programmatic login | JAAS login using authData (optional) |

---

## How It Works

### Message Flow (Step-by-Step)

1. **User Initiates Consumer**
   - HTTP GET/POST to `/control/start?topic=orders`
   - KafkaController captures the caller's Liberty Subject (security identity)
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

This sample demonstrates **per-topic listeners with individual lifecycle control**:

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
cics.transaction.map.orders=CJSU
```

If no mapping exists, the default transaction ID `CJSU` is used.

---

## Security Models Explained

This sample provides **two alternative approaches** for managing security credentials. Choose the one that best fits your operational requirements.

### Route A: Subject-Based RunAs Identity (Default - Implemented)

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
- Clear audit trail (who started which consumer)

**Code Location:**
- `KafkaController.start()` - Captures Subject
- `KafkaConsumerService.handleBatch()` - Sets RunAs Subject

**Flow Diagram:**
```
HTTP Request (authenticated)
    ↓
KafkaController captures WSSubject.getCallerSubject()
    ↓
Subject stored in topicSubjects map
    ↓
Consumer thread: WSSubject.setRunAsSubject(subject)
    ↓
ManagedExecutorService inherits RunAs identity
    ↓
CICS transaction runs as that user
```

---

### Route B: authData-Based Identity with Programmatic Login (Alternative - Not Active by Default)

**How it works:**
1. Credentials are stored in Liberty's `server.xml` as `<authData>`
2. Password is AES-encrypted using a key from a RACF keyring
3. Application performs **programmatic JAAS login** using these credentials via `LoginManager`
4. Resulting **Subject** is obtained and used the same way as Route A
5. This Subject is then set as RunAs identity on consumer threads (same mechanism as Route A)

**Key Difference from Route A:**
- **Route A:** Subject comes from authenticated HTTP request (`WSSubject.getCallerSubject()`)
- **Route B:** Subject comes from programmatic login using authData (`LoginManager.getSubject()`)
- **Both routes:** Use the same Subject-based RunAs mechanism (`WSSubject.setRunAsSubject(subject)`)

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

**Flow Diagram:**
```
Application startup
    ↓
LoginManager.getSubject()
    ↓
AuthDataProvider.getAuthData("cicsSAF")
    ↓
Liberty decrypts {aes} password using RACF key
    ↓
LoginContext.login() with credentials
    ↓
Subject cached for reuse
    ↓
Consumer thread: WSSubject.setRunAsSubject(subject)
    ↓
CICS transaction runs as authData user
```

---

## Before You Start: Files to Modify

Before building and deploying this sample, you **must** customize the following files with your environment-specific values:

### 1. Kafka Connection Configuration
**File:** `com.ibm.cicsdev.springboot.kafka.app/src/main/resources/application.properties`

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
cics.transaction.map.orders=ORDER
cics.transaction.map.test-topic=KAFK
```

---

### 2. Liberty Server Configuration
**File:** `etc/config/liberty/server.xml`

**What to change:**

**For Route A (Subject-based - default):**

```xml
<!--1. Enable features -->
<feature>cicsts:security-1.0</feature>

<!-- 2.Verify the application location matches the deployment path -->
<application location="${server.config.dir}/apps/cics-java-liberty-springboot-kafka.war" type="war">
```

**For Route B (authData-based):**
- Uncomment and configure the following sections:
```xml
<!-- 1. Enable features -->
<feature>passwordUtilities-1.0</feature>
<feature>zosPasswordEncryptionKey-1.0</feature>

<!-- 2. Configure your RACF keyring -->
<zosPasswordEncryptionKey 
    keyring="safkeyring:///<YOUR_KEYRING_NAME>" 
    label="Liberty" 
    type="JCERACFKS"/>

<!-- 3. Add authData with your credentials -->
<authData id="cicsSAF" 
    user="<YOUR_USERID>" 
    password="{aes}<YOUR_ENCRYPTED_PASSWORD>"/>
```

**Replace:**
- `<YOUR_KEYRING_NAME>` - Your RACF keyring name (e.g., `CICS.KEYRING`)
- `<YOUR_USERID>` - The RACF user ID for Kafka operations
- `<YOUR_ENCRYPTED_PASSWORD>` - Generated using `securityUtility encode` (see Route B section)

---

### 3. Build Configuration (Optional)
**Files:** 
- `com.ibm.cicsdev.springboot.kafka.bundle/build.gradle`
- `com.ibm.cicsdev.springboot.kafka.bundle/pom.xml`

**What to change:**
```gradle
// Gradle: Set your target JVM server name
cics.jvmserver = '<YOUR_JVMSERVER_NAME>'
```

```xml
<!-- Maven: Set your target JVM server name -->
<defaultjvmserver><YOUR_JVMSERVER_NAME></defaultjvmserver>
```

**Replace:**
- `<YOUR_JVMSERVER_NAME>` - The name of your CICS Liberty JVM server (e.g., `DFHWLP`)

---

### 4. Java Code (Only for Route B)
**File:** `com.ibm.cicsdev.springboot.kafka.app/src/main/java/com/example/kafkaspringboot/KafkaController.java`

**What to change:**
Uncomment the LoginManager autowiring:
```java
// Change from:
// @Autowired(required = false)
// private LoginManager loginManager;

// To:
@Autowired(required = false)
private LoginManager loginManager;
```

Then in the `start()` method, replace:
```java
Subject subject = WSSubject.getCallerSubject();
```

With:
```java
Subject subject = loginManager.getSubject();
```

**File:** `com.ibm.cicsdev.springboot.kafka.app/src/main/java/com/example/kafkaspringboot/LoginManager.java`

**What to change:**
```java
// Update the authData ID to match your server.xml
private static final String AUTH_DATA_ID = "<YOUR_AUTHDATA_ID>";
```

---

### Summary Checklist

Before building:
- [ ] Updated `application.properties` with Kafka broker address
- [ ] Configured topic-to-transaction mappings in `application.properties`
- [ ] Chose security route (A or B)
- [ ] If Route A: Updated `server.xml` with features
- [ ] If Route B: Created RACF keyring and generated AES password
- [ ] If Route B: Updated `server.xml` with features, keyring and authData
- [ ] If Route B: Uncommented LoginManager in `KafkaController.java`
- [ ] Updated JVM server name in build files (if using CICS bundle deployment)

---

## Requirements

### Workstation Requirements
* **Java:** JDK 17 or later
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
* **Java:** IBM Java 17 or later on z/OS
* **Kafka:** Apache Kafka

### Network Requirements
* Connectivity from z/OS to Kafka broker(s)
* HTTP/HTTPS access to Liberty server for REST API calls

---

## Project Structure

```
cics-java-liberty-springboot-kafka/
├── README.md                                    # This file
├── LICENSE                                      # EPL 2.0 license
├── build.gradle                                 # Root Gradle build
├── settings.gradle                              # Gradle multi-project settings
├── pom.xml                                      # Root Maven POM
├── gradlew / gradlew.bat                        # Gradle wrapper scripts
├── mvnw / mvnw.cmd                              # Maven wrapper scripts
│
├── com.ibm.cicsdev.springboot.kafka.app/        # Main application
│   ├── build.gradle                             # App-level Gradle build
│   ├── pom.xml                                  # App-level Maven POM
│   └── src/main/
│       ├── java/com/example/kafkaspringboot/
│       │   ├── KafkaApplication.java            # Spring Boot entry point
│       │   ├── KafkaController.java             # REST API for start/stop
│       │   ├── KafkaConsumerService.java        # Kafka listeners per topic
│       │   ├── KafkaMessageProcessor.java       # Async message processing
│       │   ├── KafkaBatchConfig.java            # Topic-to-transaction mapping
│       │   ├── LoginManager.java                # Optional: authData login
│       │   └── ServletInitializer.java          # WAR deployment support
│       ├── resources/
│       │   └── application.properties           # Kafka & Spring config
│       └── webapp/WEB-INF/
│           └── web.xml                          # Web app descriptor
│
├── com.ibm.cicsdev.springboot.kafka.bundle/     # CICS bundle (Gradle/Maven)
│   ├── build.gradle                             # Bundle Gradle build
│   ├── pom.xml                                  # Bundle Maven POM
│   └── src/main/bundleParts/
│       └── KAFK.transaction                     # CICS transaction definition
│
├── etc/config/
│   ├── liberty/
│   │   └── server.xml                           # Liberty server template
│   └── eclipse_projects/
│       └── com.ibm.cicsdev.springboot.examples.kafka.bundle/
│           └── ...                              # CICS Explorer bundle project
│
└── gradle/ & .mvn/                              # Wrapper support files
```

---

## Configuration Guide

### Verify CICS BOM Version

Ensure the correct CICS TS bill of materials (BOM) is specified for your target CICS release.

**Gradle** (`build.gradle`):
```gradle
compileOnly enforcedPlatform("com.ibm.cics:com.ibm.cics.ts.bom:6.3-20250905155520")
```

**Maven** (`pom.xml`):
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

Browse available versions at [Maven Central](https://search.maven.org/search?q=g:com.ibm.cics%20AND%20a:com.ibm.cics.ts.bom).

---

## Building the Sample

You can build using Gradle, Maven, or Eclipse. The wrappers are pre-configured with compatible versions.

### Option 1: Building with Gradle

**From the root directory:**

Linux/Mac:
```bash
./gradlew clean build
```

Windows:
```cmd
gradlew.bat clean build
```

**Output:**
- WAR file: `com.ibm.cicsdev.springboot.kafka.app/build/libs/cics-java-liberty-springboot-kafka.war`
- CICS bundle ZIP: `com.ibm.cicsdev.springboot.kafka.bundle/build/distributions/com.ibm.cicsdev.springboot.kafka.bundle-<version>.zip`

**Note:** In Eclipse, the `build` directory may be hidden. To view it: Package Explorer → ⋮ menu → Filters → Uncheck "Gradle build folder".

---

### Option 2: Building with Maven

**From the root directory:**

Linux/Mac:
```bash
./mvnw clean package
```

Windows:
```cmd
mvnw.cmd clean package
```

**Output:**
- WAR file: `com.ibm.cicsdev.springboot.kafka.app/target/cics-java-liberty-springboot-kafka.war`
- CICS bundle ZIP: `com.ibm.cicsdev.springboot.kafka.bundle/target/com.ibm.cicsdev.springboot.kafka.bundle-<version>.zip`

---

### Option 3: Building with Eclipse

1. **Import Projects:**
   - File → Import → General → Existing Projects into Workspace
   - Select the root directory
   - Import all projects

2. **Resolve Build Path (if needed):**
   - Right-click project → Properties → Java Build Path → Libraries
   - Add Library → CICS with Enterprise Java and Liberty
   - Select appropriate CICS and Java EE versions

3. **Build:**
   - Right-click `com.ibm.cicsdev.springboot.kafka` → Run As → Gradle Build (or Maven Build)
   - Goals: `clean build` (Gradle) or `clean package` (Maven)

**Tip:** If switching between Gradle and Maven in Eclipse, you may need to manually remove duplicate "Project Dependencies" entries from the build path.

---

## Deploying to CICS

### Method 1: CICS Bundle Deployment (Recommended)

1. **Upload the bundle ZIP to zFS:**
   ```bash
   # From your workstation
   scp com.ibm.cicsdev.springboot.kafka.bundle/build/distributions/*.zip user@zos:/path/to/bundles/
   ```

2. **Extract on z/OS:**
   ```bash
   # On z/OS
   cd /path/to/bundles
   jar xf com.ibm.cicsdev.springboot.kafka.bundle-1.0.0.zip
   ```

3. **Define CICS BUNDLE resource:**
   ```
   CEDA DEFINE BUNDLE(KAFKABUN) 
        GROUP(MYGROUP) 
        BUNDLEDIR(/path/to/bundles/com.ibm.cicsdev.springboot.kafka.bundle-1.0.0)
   ```

4. **Install the bundle:**
   ```
   CEDA INSTALL BUNDLE(KAFKABUN) GROUP(MYGROUP)
   ```

---

### Method 2: CICS Explorer Deployment

This method uses IBM CICS Explorer (an Eclipse-based IDE) to create a CICS bundle and deploy it directly to z/OS. This approach is ideal for developers who prefer a GUI-based deployment workflow and want integrated tooling for CICS development.

**Prerequisites:**
- IBM CICS Explorer installed on your workstation
- CICS Explorer configured with connection to your z/OS system
- SSH/SFTP access to z/OS UNIX System Services (USS)
- CICS region configured and running

#### Step 1: Create CICS Bundle Project in Eclipse

A CICS bundle is a deployment package that can contain multiple resources (WARs, JARs, OSGi bundles, etc.) and their metadata.

1. **Open CICS Explorer**
   - Launch Eclipse with CICS Explorer plugins installed

2. **Create New CICS Bundle Project:**
   - Navigate to: **File → New → Project...**
   - Expand **CICS** folder
   - Select **CICS Bundle Project**
   - Click **Next**

3. **Configure Bundle Project:**
   - **Project name**: `cics-springboot-kafka-bundle` (or your preferred name)
   - **Target platform**: Select your CICS TS version (e.g., CICS TS 6.3)
   - **Bundle ID**: `com.ibm.cicsdev.springboot.kafka.bundle` (must be unique in CICS region)
   - Click **Finish**

4. **Add WAR Bundle Part:**
   
   A "bundle part" is a reference to a deployable artifact within the bundle.
   
   - In **Project Explorer**, expand your bundle project
   - Right-click on the project → **New → CICS Bundle Part → WAR Bundle Part**
   - Or: Right-click on project → **New → Other... → CICS → WAR Bundle Part**
   
5. **Configure WAR Bundle Part:**
   - **Name**: `springboot-kafka-app` (this becomes the CICS PROGRAM name)
   - **JVM server**: Select or specify your Liberty JVM server name (e.g., `DFHWLP`)
   - **WAR file location**:
     - Click **Browse** or **Workspace**
     - Navigate to: `com.ibm.cicsdev.springboot.kafka.app/build/libs/cics-java-liberty-springboot-kafka.war`
     - Or use **File System** to select the WAR from your build output directory
   - Click **Finish**

6. **Review Generated Files:**
   
   The bundle project now contains:
   ```
   cics-springboot-kafka-bundle/
   ├── META-INF/
   │   └── cics.xml          # Bundle manifest (defines bundle contents)
   ├── .project              # Eclipse project file
   └── springboot-kafka-app.warbundle  # WAR bundle part descriptor
   ```

   **Understanding cics.xml:**
   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <cicsbundle xmlns="http://www.ibm.com/xmlns/prod/cics/bundle"
                version="1.0"
                id="com.ibm.cicsdev.springboot.kafka.bundle">
       <define name="springboot-kafka-app"
               type="http://www.ibm.com/xmlns/prod/cics/bundle/WAR"
               path="springboot-kafka-app.warbundle"/>
   </cicsbundle>
   ```
   - `id`: Unique identifier for this bundle in CICS
   - `define`: References the WAR bundle part
   - `path`: Location of the .warbundle descriptor file

   **Understanding .warbundle file:**
   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <warbundle symbolicname="springboot-kafka-app"
              jvmserver="DFHWLP">
       <war path="cics-java-liberty-springboot-kafka.war"/>
   </warbundle>
   ```
   - `symbolicname`: Name used to reference this WAR in CICS
   - `jvmserver`: Target Liberty JVM server name
   - `war path`: Relative path to the actual WAR file

#### Step 2: Export Bundle to z/OS UNIX File System (zFS)

This step deploys your bundle to z/OS and makes it available to CICS.

1. **Initiate Export:**
   - In **Project Explorer**, right-click on your bundle project
   - Select **Export Bundle Project to z/OS UNIX File System**
   - Click **Next**

2. **Specify Bundle Deployment Location:**
   
   Choose where on z/OS to deploy the bundle:
   - **Target directory**: `/u/cicsts/bundles/cics-springboot-kafka-bundle`
   
   **Important Path Considerations:**
   - Ensure the CICS region user has read/execute permissions
   - The directory will be created if it doesn't exist

3. **Configure Bundle Definition:**
   
   - **Bundle definition name**: `KFKABNDL` (8-character CICS resource name)
     - Must be unique in the CICS region
     - Used to install/enable/disable the bundle
   - **CICS group**: `KAFKAGRP` (optional, for resource grouping)
   - **Description**: `Spring Boot Kafka Consumer Bundle`
   
   Click **Finish** to start the export

4. **Monitor Export Progress:**
   
   The export process:
   - Connects to z/OS via SSH/SFTP
   - Creates target directory structure
   - Uploads bundle files (cics.xml, .warbundle, WAR file)
   - Sets appropriate file permissions
   - Creates CICS bundle definition (BUNDLE resource)
   
   **Console Output Example:**
   ```
   Connecting to zos.example.com...
   Creating directory /u/cicsts/bundles/cics-springboot-kafka-bundle
   Export completed successfully
   ```

#### Step 3: Install and Enable the Bundle in CICS

After export, you need to install the bundle in your CICS region:

**Option A: Using CICS Explorer**
1. In CICS Explorer, navigate to **CICS SM** (Systems Management) view
2. Expand your CICS region → **Bundle Definitions**
3. Right-click on `KFKABNDL` → **Install**
4. Right-click on `KFKABNDL` → **Enable**

**Option B: Using CICS Commands (CEMT)**
```
CEMT SET BUNDLE(KFKABNDL) INSTALL
CEMT SET BUNDLE(KFKABNDL) ENABLE
```

**Option C: Using CICS Resource Definitions (CSD)**
```
CEDA DEFINE BUNDLE(KFKABNDL)
     GROUP(KAFKAGRP)
     BUNDLEDIR(/u/cicsts/bundles/cics-springboot-kafka-bundle)
     STATUS(ENABLED)
CEDA INSTALL BUNDLE(KFKABNDL) GROUP(KAFKAGRP)
```

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
   - Expand bundle to see contained resources (WAR, PROGRAM, etc.)

---

### Method 3: Direct Liberty Application Deployment

1. **Upload WAR to zFS:**
   ```bash
   scp com.ibm.cicsdev.springboot.kafka.app/build/libs/*.war user@zos:/path/to/liberty/apps/
   ```

2. **Add to server.xml:**
   ```xml
   <application location="${server.config.dir}/apps/cics-java-liberty-springboot-kafka.war" type="war">
       <application-bnd>
           <security-role name="cics-user">
               <special-subject type="ALL_AUTHENTICATED_USERS"/>
           </security-role>
       </application-bnd>
   </application>
   ```

3. **Restart Liberty or refresh configuration**

---

## Running the Sample

### Step 1: Verify Deployment

Check Liberty messages.log for successful application start:
```
[AUDIT   ] CWWKT0016I: Web application available (default_host): http://hostname:9080/cics-java-liberty-springboot-kafka/
```

---

### Step 2: Start a Kafka Consumer

**Using curl:**
```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/start?topic=test-topic" \
     -u username:password
```

**Using a browser:**
```
http://hostname:9080/cics-java-liberty-springboot-kafka/control/start?topic=test-topic
```
(Browser will prompt for credentials)

**Expected Response:**
```
Started listener for topic=test-topic
```

---

### Step 3: Send Test Messages to Kafka

**Using kafka-console-producer:**
```bash
kafka-console-producer --broker-list <broker>:9092 --topic test-topic
> Hello from Kafka!
> This is a test message
```

---

### Step 4: Verify Message Processing

Check Liberty messages.log:
```
[INFO] Received message from topic test-topic: Hello from Kafka!
[INFO] Task USERID = TESTUSER
[INFO] DEBUG: Topic = test-topic
[INFO] DEBUG: Finished processing Kafka message in thread: Default Executor-thread-1 Hello from Kafka!
```

Check CICS for transaction execution:
```
CEMT I TASK
```
You should see tasks running with transaction ID `KAFK` (or whatever you configured).

---

### Step 5: Stop the Consumer

```bash
curl -X POST "http://hostname:9080/cics-java-liberty-springboot-kafka/control/stop?topic=test-topic" \
     -u username:password
```

**Expected Response:**
```
Stopped listener for topic=test-topic
```

---

### Multiple Topics

You can run multiple consumers simultaneously:
```bash
# Start consumer for orders topic
curl -X POST ".../control/start?topic=orders" -u username:pass

# Start consumer for test-topic
curl -X POST ".../control/start?topic=test-topic" -u username:pass

# Each runs independently with its own transaction ID
```

---

## Understanding the Code

This section provides detailed explanations of key code patterns for educational purposes.

### 1. Spring Boot Entry Point

**File:** `KafkaApplication.java`

```java
@SpringBootApplication
public class KafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }
}
```

**What it does:**
- `@SpringBootApplication` combines `@Configuration`, `@EnableAutoConfiguration`, and `@ComponentScan`
- When deployed as WAR, `ServletInitializer` takes over initialization

---

### 2. REST Controller for Lifecycle Management

**File:** `KafkaController.java`

**Key Pattern: Capturing Security Context**
```java
@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
public ResponseEntity<String> start(@RequestParam String topic) throws Exception {
    // 1. Capture the authenticated user's Subject
    Subject subject = WSSubject.getCallerSubject();
    
    // 2. Store it for this topic
    topicSubjects.put(topic, subject);
    
    // 3. Start the Kafka listener
    String listenerId = listenerIdFor(topic);
    var container = registry.getListenerContainer(listenerId);
    container.start();
    
    return ResponseEntity.ok("Started listener for topic=" + topic);
}
```

**Why this matters:**
- The Subject represents the authenticated user who called `/start`
- This identity will be used for all CICS transactions processing messages from this topic
- Different topics can run under different identities (different users call `/start`)

---

### 3. Kafka Listeners with Security Propagation

**File:** `KafkaConsumerService.java`

**Key Pattern: Setting RunAs Subject**
```java
@KafkaListener(id = "ordersListener", topics = "orders", groupId = "test-group", 
    containerFactory = "batchFactory", autoStartup = "false")
public void onOrdersBatch(List<ConsumerRecord<String, String>> batch) {
    handleBatch("orders", batch);
}

private void handleBatch(String topic, List<ConsumerRecord<String, String>> batch) {
    // 1. Get the Subject captured at /start
    Subject subject = control.getTopicSubjects().get(topic);
    
    // 2. Set RunAs ONCE per consumer thread (first batch only)
    if (!runAsInitialized.get()) {
        WSSubject.setRunAsSubject(subject);
        runAsInitialized.set(true);
    }
    
    // 3. Process each message
    for (ConsumerRecord<String, String> rec : batch) {
        processor.processAsynchronous(rec);
    }
}
```

**Why this matters:**
- Spring Kafka creates long-lived consumer threads
- We set the RunAs identity once when the batch arrives
- All subsequent work on this thread inherits this identity
- Liberty's ManagedExecutorService will propagate this to worker threads

---

### 4. Asynchronous Processing with ManagedExecutorService

**File:** `KafkaMessageProcessor.java`

**Key Pattern: CICS-Aware Thread Execution**
```java
@Service
public class KafkaMessageProcessor {
    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;
    
    public void processAsynchronous(ConsumerRecord<String, String> record) {
        // Submit to Liberty's managed thread pool
        executor.submit(new KafkaCICSTransactionRunnable(record, config));
    }
    
    private static class KafkaCICSTransactionRunnable implements CICSTransactionRunnable {
        @Override
        public void run() {
            // This runs on a CICS-aware thread
            Task task = Task.getTask();
            // ... process message ...
        }
        
        @Override
        public String getTranid() {
            // Map topic to transaction ID
            return config.getTranIdForTopic(record.topic());
        }
    }
}
```

**Why this matters:**
- `ManagedExecutorService` is provided by Liberty (Jakarta Concurrency)
- It creates threads that are CICS-aware (can call `Task.getTask()`)
- `CICSTransactionRunnable` ensures work runs in a CICS transaction
- The transaction ID is determined dynamically based on the topic

---

### 5. Topic-to-Transaction Mapping

**File:** `KafkaBatchConfig.java`

**Key Pattern: Externalized Configuration**
```java
@Configuration
@ConfigurationProperties(prefix = "cics.transaction")
public class KafkaBatchConfig {
    private Map<String, String> map = new HashMap<>();
    
    public String getTranIdForTopic(String topic) {
        return map.getOrDefault(topic, "CJSU");
    }
}
```

**Configuration in application.properties:**
```properties
cics.transaction.map.orders=ORDR
cics.transaction.map.test-topic=KAFK
```

**Teaching Point:** Use Spring's `@ConfigurationProperties` to externalize configuration. This makes the application more flexible and maintainable.

---

### 6. Alternative: Programmatic Login (Route B)

**File:** `LoginManager.java`

**Key Pattern: JAAS Login with authData**
```java
public class LoginManager {
    private static final String AUTH_DATA_ID = "cicsSAF";
    private volatile Subject cachedSubject;
    
    public Subject getSubject() {
        if (cachedSubject == null) {
            synchronized (this) {
                if (cachedSubject == null) {
                    cachedSubject = loginUsingAuthDataUserPassword(AUTH_DATA_ID);
                }
            }
        }
        return cachedSubject;
    }
    
    private Subject loginUsingAuthDataUserPassword(String alias) {
        // 1. Get credentials from server.xml
        AuthData ad = AuthDataProvider.getAuthData(alias);
        String user = ad.getUserName();
        char[] pwdChars = ad.getPassword(); // Liberty decrypts {aes} password
        
        // 2. Perform JAAS login
        LoginContext lc = new LoginContext("system.DEFAULT", 
            new WSCallbackHandlerImpl(user, new String(pwdChars)));
        lc.login();
        return lc.getSubject();
    }
}
```

**Why this matters:**
- Credentials are stored securely in server.xml (AES-encrypted)
- No passwords in application code
- Subject is cached to avoid expensive repeated logins
- Double-checked locking ensures thread-safe lazy initialization.

---

### 7. Batch Processing Configuration

**File:** `KafkaBatchConfig.java`

**Key Pattern: Custom Container Factory**
```java
@Bean(name = "batchFactory")
public ConcurrentKafkaListenerContainerFactory<String, String> batchFactory(
    ConsumerFactory<String, String> consumerFactory) {
    
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumerFactory);
    
    // Enable batch delivery
    factory.setBatchListener(true);
    
    // Scale with multiple consumer threads
    factory.setConcurrency(3);
    
    return factory;
}
```

**Why this matters:**
- Batch processing is more efficient than processing one message at a time
- `setConcurrency(3)` creates 3 consumer threads per topic
- Spring Boot's auto-configured ConsumerFactory is reused (DRY principle)

---

## Troubleshooting

### Issue: Messages not being consumed

**Symptom:** No log messages after calling `/start`

**Possible Causes:**
1. **Kafka connectivity:** Verify network access to broker
2. **Topic doesn't exist:** Create the topic in Kafka
3. **Consumer group offset:** Consumer may be at end of topic
4. **Authentication failed:** Check Subject was captured correctly

**Debugging Steps:**
```bash
# Check Kafka connectivity from z/OS
telnet <broker-ip> 9092

# List topics
kafka-topics --bootstrap-server <broker>:9092 --list

# Check consumer group status
kafka-consumer-groups --bootstrap-server <broker>:9092 --group test-group --describe

# Check Liberty logs
tail -f /path/to/liberty/logs/messages.log
```

---

### Issue: "Failed to set RunAsSubject"

**Symptom:** WSSecurityException in logs

**Cause:** Subject is null or invalid

**Solution:**
- Verify user is authenticated when calling `/start`
- Check Liberty security configuration
- For Route B: Verify authData is configured correctly

---

### Issue: Wrong transaction ID being used

**Symptom:** Messages processed under unexpected transaction

**Cause:** Topic mapping not configured

**Solution:**
- Check `application.properties` for `cics.transaction.map.<topic>=<tranid>`
- Verify `KafkaBatchConfig` is loading the configuration
- Default is `CJSU` if no mapping exists

---

### Issue: AES password decryption fails (Route B)

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

This project is licensed under **Eclipse Public License - v 2.0**.

### Usage Terms

By downloading, installing, and/or using this sample, you acknowledge that separate license terms may apply to any dependencies required for installation, execution, or automated builds, including:

**IBM CICS development components:**
https://www.ibm.com/support/customer/csol/terms/?id=L-ACRR-BBZLGX

---

## Additional Resources

- [CICS Liberty Documentation](https://www.ibm.com/docs/en/cics-ts/latest?topic=liberty-cics)
- [Spring Boot Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

---

## Contributing

This is a sample project maintained by IBM CICS development. For issues or questions:
- Open an issue on GitHub
- Contact IBM Support for CICS-related questions

---

**Last Updated:** March 2026
**Version:** 1.0.0
**Maintainers:** See MAINTAINERS.md
