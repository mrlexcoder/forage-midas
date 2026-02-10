# Midas Core - Setup Fixes Documentation

## Problem Summary

The Spring Boot application was failing to start during tests with `IllegalStateException: Failed to load ApplicationContext`. All test classes (TaskOneTests through TaskFiveTests) were failing.

## Root Cause

The main `application.yml` configuration file in the project root was **empty**, causing Spring Boot to fail during context initialization. The application needed:
1. Kafka topic configuration
2. Kafka serialization settings for JSON objects
3. Database configuration
4. JPA/Hibernate settings

## Solution Applied

### Created `application.yml` with Complete Configuration

```yaml
general:
  kafka-topic: trader-updates

spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"

  datasource:
    url: jdbc:h2:mem:testdb
    driver-class-name: org.h2.Driver
    username: sa
    password:

  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: false
```

## Configuration Breakdown

### 1. General Configuration
- **kafka-topic**: Set to `trader-updates` as required by Task 1 instructions

### 2. Kafka Configuration
- **bootstrap-servers**: Points to localhost:9092 for local Kafka broker
- **Producer Settings**:
  - `key-serializer`: StringSerializer for message keys
  - `value-serializer`: JsonSerializer to serialize Transaction objects as JSON
- **Consumer Settings**:
  - `key-deserializer`: StringDeserializer for message keys
  - `value-deserializer`: JsonDeserializer to deserialize JSON back to Transaction objects
  - `spring.json.trusted.packages`: Set to "*" to allow deserialization of all packages

### 3. Database Configuration (H2)
- **url**: In-memory H2 database named `testdb`
- **driver-class-name**: H2 JDBC driver
- **username**: `sa` (default H2 admin user)
- **password**: Empty (no password required for test database)

### 4. JPA/Hibernate Configuration
- **ddl-auto**: `create-drop` - Creates schema on startup, drops on shutdown (ideal for testing)
- **show-sql**: `false` - Disables SQL logging to keep test output clean

## Why JSON Serialization Was Needed

The `KafkaProducer` class uses `KafkaTemplate<String, Transaction>`, which means it sends Transaction objects (not strings) to Kafka. The Transaction class is a POJO with:
- `senderId` (long)
- `recipientId` (long)
- `amount` (float)

Without JSON serialization configured, Kafka tried to use StringSerializer for Transaction objects, causing `SerializationException`.

## Test Results

### Before Fix
```
[ERROR] Failed to load ApplicationContext
[ERROR] Tests run: 5, Failures: 0, Errors: 5, Skipped: 0
```

### After Fix
```
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

### Task 1 Output
```
---begin output ---
1142725631254665682354316777216387420489
---end output ---
```

## Files Modified

1. **application.yml** - Created with complete Spring Boot configuration

## Dependencies Already Present

The `pom.xml` already had all required dependencies:
- spring-boot-starter-web (3.2.5)
- spring-boot-starter-data-jpa (3.2.5)
- spring-kafka (3.1.4)
- h2 database (2.2.224)
- spring-boot-starter-test (3.2.5)
- spring-kafka-test (3.1.4)
- testcontainers-kafka (1.19.1)

## Next Steps

With the configuration fixed, you can now:
1. Submit the Task 1 output: `1142725631254665682354316777216387420489`
2. Proceed with implementing the remaining tasks (Task 2-5)
3. Run all tests with: `mvn test`
4. Run specific test with: `mvn -Dtest=TaskOneTests test`

## Key Takeaway

Always ensure your `application.yml` or `application.properties` file contains the necessary configuration for Spring Boot to initialize the ApplicationContext, especially when working with:
- Kafka messaging
- Database connections
- Custom properties referenced in `@Value` annotations
