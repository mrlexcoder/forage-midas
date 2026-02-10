# Task 2 Solution: Kafka Integration

## Overview
This document explains how to integrate Kafka into Midas Core to consume transaction messages from the `trader-updates` topic.

## Problem Statement
Implement a Kafka listener that:
- Listens to the `trader-updates` topic configured in `application.yml`
- Deserializes incoming messages to Transaction objects
- Processes transactions as they arrive

## Solution Steps

### Step 1: Verify Application Configuration
**File**: `application.yml` (project root)

Ensure the following Kafka configuration exists:

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
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

**Key Configuration Points**:
- `auto-offset-reset: earliest` - Ensures consumer reads from the beginning of the topic
- `JsonDeserializer` - Automatically deserializes JSON messages to Java objects
- `spring.json.trusted.packages: "*"` - Allows deserialization of all packages (for testing)

### Step 2: Create Kafka Consumer Component
**File**: `src/main/java/com/jpmc/midascore/component/KafkaConsumer.java`

Create a new component class with the `@KafkaListener` annotation:

```java
package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: senderId={}, recipientId={}, amount={}", 
                    transaction.getSenderId(), 
                    transaction.getRecipientId(), 
                    transaction.getAmount());
        
        // Process transaction here
    }
}
```

**Key Components**:
- `@Component` - Marks this as a Spring-managed bean
- `@KafkaListener` - Configures the method as a Kafka message listener
- `topics = "${general.kafka-topic}"` - References the topic from application.yml
- `groupId = "midas-core-group"` - Consumer group identifier
- Method parameter `Transaction transaction` - Spring automatically deserializes JSON to this object

### Step 3: Understand the Transaction Class
**File**: `src/main/java/com/jpmc/midascore/foundation/Transaction.java`

The Transaction class is already provided:

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {
    private long senderId;
    private long recipientId;
    private float amount;
    
    // Constructors, getters, setters...
}
```

The `@JsonIgnoreProperties` annotation allows flexible JSON deserialization.

### Step 4: Run the Test
Execute the test to verify the Kafka integration:

```bash
mvn -Dtest=TaskTwoTests test
```

**What Happens**:
1. Spring Boot starts with embedded Kafka
2. Test loads transactions from `src/test/resources/test_data/poiuytrewq.uiop`
3. KafkaProducer sends all transactions to the `trader-updates` topic
4. Your KafkaConsumer receives and logs each transaction
5. Test enters infinite loop (by design) - you must manually kill it

### Step 5: Extract the Answer
Look at the console output for the first 4 transactions received:

```
Received transaction: senderId=6, recipientId=7, amount=122.86
Received transaction: senderId=5, recipientId=2, amount=42.87
Received transaction: senderId=7, recipientId=4, amount=161.79
Received transaction: senderId=8, recipientId=7, amount=22.22
```

**Answer**: The first 4 transaction amounts are:
1. 122.86
2. 42.87
3. 161.79
4. 22.22

## How It Works

### Kafka Message Flow
```
Test Data File (poiuytrewq.uiop)
    ↓
KafkaProducer (Test Helper)
    ↓
Kafka Topic: trader-updates
    ↓
KafkaConsumer (Your Implementation)
    ↓
Transaction Processing
```

### Spring Kafka Auto-Configuration
Spring Boot automatically:
- Creates KafkaTemplate for producers
- Creates ConsumerFactory for consumers
- Handles JSON serialization/deserialization
- Manages consumer group coordination
- Handles offset management

### Consumer Group Behavior
- `groupId = "midas-core-group"` ensures all consumers in this group share the workload
- With `auto-offset-reset: earliest`, new consumers start from the beginning
- Kafka tracks which messages each consumer group has processed

## Testing with Embedded Kafka

The test uses `@EmbeddedKafka` annotation:

```java
@EmbeddedKafka(partitions = 1, brokerProperties = {
    "listeners=PLAINTEXT://localhost:9092", 
    "port=9092"
})
```

This creates an in-memory Kafka broker for testing without external dependencies.

## Common Issues and Solutions

### Issue 1: Consumer Not Receiving Messages
**Solution**: Ensure `auto-offset-reset: earliest` is set in application.yml

### Issue 2: Deserialization Errors
**Solution**: Verify `spring.json.trusted.packages: "*"` is configured

### Issue 3: Test Hangs
**Expected Behavior**: The test is designed to run indefinitely. Kill it manually after observing the transactions.

## Next Steps

After completing this task, you can:
1. Add business logic to process transactions
2. Store transactions in the database
3. Implement error handling for failed transactions
4. Add transaction validation logic

## Files Modified

1. `application.yml` - Kafka configuration
2. `src/main/java/com/jpmc/midascore/component/KafkaConsumer.java` - New file created

## Dependencies Used

All required dependencies are already in `pom.xml`:
- `spring-kafka` - Spring Kafka integration
- `spring-kafka-test` - Embedded Kafka for testing
- `jackson-databind` - JSON serialization/deserialization
