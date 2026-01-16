# cics-java-liberty-springboot-kafka
[![Build](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml/badge.svg)](https://github.com/cicsdev/cics-java-liberty-springboot-kafka/actions/workflows/java.yaml)

This project demonstrates a Spring Boot–based Kafka consumer integrated with IBM CICS and deployed as a WAR to a CICS Liberty JVM server on z/OS. The application processes Kafka messages asynchronously under the caller’s security context and avoids clear-text credentials by storing them as AES-encrypted values in Liberty server.xml, with the AES key held in a RACF key ring. The sample includes both Gradle and Maven build configurations for use in Eclipse or standalone build environments.It also demonstrates configurable workload classification onto CICS transactions (as well as security).

- [etc/config/liberty/server.xml](./etc/config/liberty/server.xml) - A template `server.xml` demonstrating the minimum configuration required to run the sample.
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

## Downloading
Clone the repository using your IDEs support, such as the Eclipse Git plugin
or, download the sample as a ZIP and unzip onto the workstation
>*Tip: Eclipse Git provides an 'Import existing Projects' check-box when cloning a repository.*

### Check dependencies
If you are building this sample with Gradle or Maven you should verify that the correct CICS TS bill of materials (BOM) is specified for your target release of CICS. The BOM specifies a consistent set of artifacts, and adds information about their scope. In the example below the version specified is compatible with CICS TS V6.3, or newer. You can browse the published versions of the CICS BOM at Maven Central.

Gradle (build.gradle):

compileOnly enforcedPlatform("com.ibm.cics:com.ibm.cics.ts.bom:6.3-20250905155520")

Maven (POM.xml):

``` xml
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

## Building the sample

You can build the sample using an IDE of your choice, or you can build it from the command line. For both approaches, using the supplied Gradle or Maven wrapper is the recommended way to get a consistent version of build tooling. 

On the command line, you simply swap the Gradle or Maven command for the wrapper equivalent, `gradlew` or `mvnw` respectively.
  
For an IDE, taking Eclipse as an example, the plug-ins for Gradle *buildship* and Maven *m2e* will integrate with the "Run As..." capability, allowing you to specify whether you want to build the project with a Wrapper, or a specific version of your chosen build tool.

The required build-tasks are typically `clean bootWar` for Gradle and `clean package` for Maven. Once run, Gradle will generate a WAR file in the `build/libs` directory, while Maven will generate it in the `target` directory.

**Note:** When building a WAR file for deployment to Liberty it is good practice to exclude Tomcat from the final runtime artifact. We demonstrate this in the pom.xml with the *provided* scope, and in build.gradle with the *providedRuntime()* dependency.

*Note:** If you are building and deploying with Gradle or Maven then you don't necessarily need to fix the local errors, but to do so, you can do as above, or you can run a tooling refresh on the cics-java-liberty-springboot-kafka project. For example, in Eclipse: right-click on "Project", select "Gradle -> Refresh Gradle Project", or right-click on "Project", select "Maven -> Update Project..."..

>Tip: *In Eclipse, Gradle (buildship) is able to fully refresh and resolve the local classpath even if the project was previously updated by Maven. However, Maven (m2e) does not currently reciprocate that capability. If you previously refreshed the project with Gradle, you'll need to manually remove the 'Project Dependencies' entry on the Java build-path of your Project Properties to avoid duplication errors when performing a Maven Project Update.*  

#### Gradle Wrapper (command line)

Run the following in a local command prompt:

On Linux or Mac:

```shell
./gradlew clean bootWar
```
On Windows:

```shell
gradlew.bat clean bootWar
```

This creates a WAR file inside the `build/libs` directory.

**Minimum Maven Version**: 3.8.1+ (Java 17 support)
The Maven wrapper included in this project uses Maven 3.9.x, which fully supports Java 17-21.

#### Maven Wrapper (command line)

Run the following in a local command prompt:

On Linux or Mac:

```shell
./mvnw clean package
```

On Windows:

```shell
mvnw.cmd clean package
```

This creates a WAR file inside the `target` directory.

**Minimum Gradle Version**: 7.3+ (Java 17 support)
The Gradle wrapper included in this project uses Gradle 8.x, which fully supports Java 17-21.

---

## Deploying to a CICS Liberty JVM server

- Ensure you have the following features defined in your Liberty `server.xml`:           
    - `<servlet-6.0>` depending on the version of Java EE in use.  
    - `<cicsts:security-1.0>` if CICS security is enabled.

``` XML
<featureManager>
        <feature>appSecurity-5.0</feature>
        <feature>cicsts:security-1.0</feature>
        <feature>concurrent-3.0</feature>
        <feature>servlet-6.0</feature>
        <feature>jdbc-4.3</feature>
</featureManager>
```

Deployment option 1:

Copy and paste the built WAR from your target or build/libs directory into a Eclipse CICS bundle project and create a new WAR bundlepart that references the WAR file. Then deploy the CICS bundle project from CICS Explorer using the Export Bundle Project to z/OS UNIX File System wizard.

Deployment option 2:

Manually upload the WAR file to zFS and add an <application> element to the Liberty server.xml to define the web application with access to all authenticated users. For example the following application element can be used to install a WAR, and grant access to all authenticated users if security is enabled.

``` XML
    <application location="${server.config.dir}/apps/cics-java-liberty-springboot-kafka.war" type="war">
        <application-bnd>
            <security-role name="cics-user">
                <special-subject type="ALL_AUTHENTICATED_USERS"/>
            </security-role>
        </application-bnd>
    </application>
```    

---

## Running the sample

1. Configure the Kafka connection details and credentials as required.
2. Update the Liberty `server.xml` if needed (features, security configuration).
3. Start the Liberty server and deploy the application.

Kafka consumers will start on application-managed background threads during application initialisation.

Make the kafka consumer start/stop using - 
```XML
http://<url>/cics-java-liberty-springboot-kafka/control/start?topic=<topic-name> or http://<url>/cics-java-liberty-springboot-kafka/control/stop?topic=<topic-name>
```

---

### Logging Strategy: Java Util Logging (JUL)

The application uses **Java Util Logging (JUL)** exclusively.

#### Why JUL?

* Built into the JDK (no additional dependencies)
* Integrated with Liberty’s logging infrastructure
* Thread-safe and efficient
* Appropriate for sample code where simplicity and clarity matter

#### Why not `System.out.println` / `System.err.println`?

* Not synchronised
* Poor performance under concurrency
* Bypasses Liberty logging configuration
* Makes diagnostics harder in managed environments

#### Why not Log4j / SLF4J / Logback?

* Introduces unnecessary dependencies for a sample
* Adds logging bridge and classloader complexity
* Distracts from Liberty’s native logging model

#### Important note on output

By default, JUL output appears in messages.log when the console log level permits.

Ensure the following JVM option is set:
  -Dcom.ibm.ws.logging.console.log.level=INFO

This is documented intentionally so users understand Liberty’s logging behaviour.

---

## Security Model Demonstrated

This sample demonstrates explicit, application-managed security identity propagation for Kafka consumer threads running in WebSphere Liberty.
Kafka consumers execute on application-managed background threads, so security identity must be established explicitly rather than being inherited from a container-managed request thread.

Two supported approaches are documented:

Option A: Subject-based RunAs identity (default)
Option B: authData-based identity (alternative)

Only one option should be used at a time.

Option A: Subject-Based RunAs Identity (Default)

This is the default approach implemented in the sample code. The application explicitly establishes a RunAs identity on Kafka consumer threads using Liberty security APIs.

Key characteristics:

A Subject representing the service identity is obtained during application initialisation
The identity is applied using WSSubject.setRunAsSubject(subject)
The RunAs subject is set once per consumer thread and reused for subsequent processing
Previous identity state is captured and managed explicitly

This approach:

* Works well with long-lived Kafka consumer threads
* Provides full control over credentials per topic
* Supports **“service ID per topic”** models
* Makes identity propagation explicit and observable

Option B: authData-Based Identity (Alternative)

As an alternative to Subject-based RunAs identity, the sample can be configured to use Liberty authData to associate credentials with outbound Kafka connections.

In this model:

Service credentials are defined declaratively in server.xml
The application references the configured authData by name
Liberty manages credential lookup and association
No explicit Subject or RunAs switching is performed in application code

This approach:

Centralises credential management in server configuration
Reduces application-level security handling
Is preferred when credentials must not appear in application code
Aligns well with operationally managed environments

server.xml configuration (Option B only)

Option B only: authData-based Kafka credentials        

```XML
<server>
  <featureManager>
    <feature>appSecurity-5.0</feature>
    <feature>cicsts:security-1.0</feature>
    <feature>concurrent-3.0</feature>
    <feature>servlet-6.0</feature>
    <feature>jdbc-4.3</feature>
    <feature>passwordUtilities-1.0</feature>
    <feature>zosPasswordEncryptionKey-1.0</feature>
  </featureManager>
  

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

  <!-- Credentials alias (AES-protected) -->
  <authData id="cics_Auth_Id" user="<user_id>" password="{aes}..."/>

</server>

```
-  Create the RACF key ring and certificates (label = `Liberty`)

> Run these TSO commands with appropriate IDs and DNs for your environment.
> YOUR.KEYRING is the generic value for keyring.

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
                                                                          
    Digital ring information for user IN00501:                                  
                                                                                
      Ring:                                                                     
          >YOUR.KEYRING<                                                      
      Certificate Label Name             Cert Owner     USAGE      DEFAULT      
      --------------------------------   ------------   --------   -------      
      LIBERTY.CA                            CERTAUTH    CERTAUTH     NO         
      Liberty                            ID(<user_id>)    PERSONAL     YES 

```

Reference: JCERACFKS/JCECCARACFKS examples and ring setup [IBM Docs](https://www.ibm.com/docs/en/was-liberty/nd?topic=ssl-configuring-keyring-based-keystore).

Generate the AES‑encoded password with `securityUtility` (key from RACF)

From `${wlp.install.dir}/wlp/bin`, or from SSH shell on zFS to the same, run:

```bash
securityUtility encode \
  --encoding=aes \
  --keyring=safkeyring:///YOUR.KEYRING \
  --keyringType=JCERACFKS \
  --keyLabel=Liberty \
  "YourRacfPassword"

```

<!-- Do not configure authData when using Option A -->

Kafka client configuration should reference the above authData entry when this option is selected.

---

## License

This project is licensed under Eclipse Public License - v 2.0.

Usage terms
By downloading, installing, and/or using this sample, you acknowledge that separate license terms may apply to any dependencies that might be required as part of the installation and/or execution and/or automated build of the sample, including the following IBM license terms for relevant IBM components:

• IBM CICS development components terms: https://www.ibm.com/support/customer/csol/terms/?id=L-ACRR-BBZLGX
