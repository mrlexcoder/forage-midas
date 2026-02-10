# Task 3 Solution: Database Integration with Transaction Validation

## Overview
This document explains how to integrate an H2 database into Midas Core to validate and persist financial transactions with proper balance management.

## Problem Statement
Implement database integration that:
- Validates transactions based on sender/recipient existence and sufficient balance
- Creates a TransactionRecord entity with many-to-one relationships to UserRecord
- Updates sender and recipient balances when transactions are valid
- Discards invalid transactions without modifying the database

## Solution Steps

### Step 1: Create TransactionRecord Entity
**File**: `src/main/java/com/jpmc/midascore/entity/TransactionRecord.java`

Create a new JPA entity to represent persisted transactions:

```java
package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue
    private Long id;

    @ManyToOne
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord sender;

    @ManyToOne
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipient;

    @Column(nullable = false)
    private float amount;

    protected TransactionRecord() {
    }

    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
    }

    // Getters and toString()...
}
```

**Key Components**:
- `@Entity` - Marks this as a JPA entity
- `@ManyToOne` - Establishes many-to-one relationship (many transactions to one user)
- `@JoinColumn` - Specifies the foreign key column name
- Separate from `Transaction` class (which is for Kafka deserialization)

### Step 2: Create TransactionRepository
**File**: `src/main/java/com/jpmc/midascore/repository/TransactionRepository.java`

Create a repository interface for TransactionRecord:

```java
package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.data.repository.CrudRepository;

public interface TransactionRepository extends CrudRepository<TransactionRecord, Long> {
}
```

Spring Data JPA automatically implements this interface with CRUD operations.

### Step 3: Update UserRepository
**File**: `src/main/java/com/jpmc/midascore/repository/UserRepository.java`

Add a method to find users by name (useful for debugging):

```java
package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<UserRecord, Long> {
    UserRecord findById(long id);
    UserRecord findByName(String name);
}
```

### Step 4: Update DatabaseConduit
**File**: `src/main/java/com/jpmc/midascore/component/DatabaseConduit.java`

Add methods to support transaction processing:

```java
package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository, 
                          TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public UserRecord findUserById(long id) {
        return userRepository.findById(id);
    }

    public UserRecord findUserByName(String name) {
        return userRepository.findByName(name);
    }

    public void saveTransaction(TransactionRecord transactionRecord) {
        transactionRepository.save(transactionRecord);
    }

    public void updateUser(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
}
```

### Step 5: Implement Transaction Validation and Processing
**File**: `src/main/java/com/jpmc/midascore/component/KafkaConsumer.java`

Update the Kafka consumer to validate and process transactions:

```java
package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;

    public KafkaConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: senderId={}, recipientId={}, amount={}", 
                    transaction.getSenderId(), 
                    transaction.getRecipientId(), 
                    transaction.getAmount());
        
        // Validate and process the transaction
        if (isValidTransaction(transaction)) {
            processTransaction(transaction);
            logger.info("Transaction processed successfully");
        } else {
            logger.warn("Transaction discarded - validation failed");
        }
    }

    private boolean isValidTransaction(Transaction transaction) {
        // Check if sender exists
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return false;
        }

        // Check if recipient exists
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return false;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance: sender={}, balance={}, amount={}", 
                       sender.getName(), sender.getBalance(), transaction.getAmount());
            return false;
        }

        return true;
    }

    private void processTransaction(Transaction transaction) {
        // Get sender and recipient
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save updated users
        databaseConduit.updateUser(sender);
        databaseConduit.updateUser(recipient);

        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(
            sender, recipient, transaction.getAmount()
        );
        databaseConduit.saveTransaction(transactionRecord);

        logger.info("Updated balances - Sender: {} ({}), Recipient: {} ({})",
                   sender.getName(), sender.getBalance(),
                   recipient.getName(), recipient.getBalance());
    }
}
```

**Validation Logic**:
1. Check if senderId exists in database
2. Check if recipientId exists in database
3. Check if sender has sufficient balance (balance >= amount)
4. If all checks pass, process the transaction
5. If any check fails, discard the transaction

**Processing Logic**:
1. Retrieve sender and recipient from database
2. Deduct amount from sender's balance
3. Add amount to recipient's balance
4. Save both updated user records
5. Create and save TransactionRecord with relationships

### Step 6: Run the Test
Execute the test to process transactions:

```bash
mvn -Dtest=TaskThreeTests test
```

**What Happens**:
1. Test populates database with users from `lkjhgfdsa.hjkl`:
   - waldorf starts with balance: 444.55
2. Test sends transactions from `mnbvcxz.vbnm` via Kafka
3. KafkaConsumer validates and processes each transaction
4. Valid transactions update balances and are persisted
5. Invalid transactions are discarded

### Step 7: Find Waldorf's Final Balance

Look at the console output for transactions involving waldorf:

```
Updated balances - Sender: wilbur (3608.88), Recipient: waldorf (489.96997)
Updated balances - Sender: whosit (615.69), Recipient: waldorf (522.08997)
Updated balances - Sender: waldorf (443.34998), Recipient: wilbur (3687.6199)
Updated balances - Sender: wilbur (3572.6697), Recipient: waldorf (627.86)
```

**Transaction Flow for Waldorf**:
1. Starting balance: 444.55
2. Received 45.42 from wilbur → 489.97
3. Received 32.12 from whosit → 522.09
4. Sent 78.74 to wilbur → 443.35
5. Received 184.51 from wilbur → 627.86

**Final Balance**: 627.86
**Rounded Down**: **627**

## How It Works

### JPA Relationships
```
UserRecord (One)
    ↑
    | @ManyToOne
    |
TransactionRecord (Many)
    |
    | @ManyToOne
    ↓
UserRecord (One)
```

Each TransactionRecord has:
- One sender (UserRecord)
- One recipient (UserRecord)

Each UserRecord can have:
- Many transactions as sender
- Many transactions as recipient

### Transaction Processing Flow
```
Kafka Message
    ↓
KafkaConsumer.listen()
    ↓
isValidTransaction()
    ├─ Check sender exists
    ├─ Check recipient exists
    └─ Check sufficient balance
    ↓
processTransaction()
    ├─ Update sender balance (subtract)
    ├─ Update recipient balance (add)
    ├─ Save both users
    └─ Create and save TransactionRecord
```

### Database Schema
```sql
-- UserRecord table
CREATE TABLE user_record (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    balance FLOAT NOT NULL
);

-- TransactionRecord table
CREATE TABLE transaction_record (
    id BIGINT PRIMARY KEY,
    sender_id BIGINT NOT NULL,
    recipient_id BIGINT NOT NULL,
    amount FLOAT NOT NULL,
    FOREIGN KEY (sender_id) REFERENCES user_record(id),
    FOREIGN KEY (recipient_id) REFERENCES user_record(id)
);
```

## Validation Rules

### Valid Transaction
All of the following must be true:
- ✅ Sender ID exists in database
- ✅ Recipient ID exists in database
- ✅ Sender balance >= transaction amount

### Invalid Transaction Examples
- ❌ Sender ID = 999 (doesn't exist)
- ❌ Recipient ID = 888 (doesn't exist)
- ❌ Sender balance = 100, amount = 150 (insufficient funds)

## Why Use Separate Transaction Classes?

**Transaction** (foundation package):
- Used for Kafka message deserialization
- Simple POJO with senderId, recipientId, amount
- No JPA annotations
- Represents incoming data

**TransactionRecord** (entity package):
- Used for database persistence
- JPA entity with relationships
- References actual UserRecord objects
- Represents persisted data with full context

This separation follows the principle of separating concerns:
- Data Transfer Objects (DTOs) for messaging
- Entities for persistence

## Common Issues and Solutions

### Issue 1: Transaction Not Persisted
**Cause**: Validation failed
**Solution**: Check logs for validation errors (invalid IDs or insufficient balance)

### Issue 2: Balance Not Updated
**Cause**: User not saved after balance change
**Solution**: Ensure `updateUser()` is called after modifying balance

### Issue 3: Foreign Key Constraint Violation
**Cause**: Trying to save TransactionRecord with non-existent user
**Solution**: Always validate user existence before creating TransactionRecord

## Testing Strategy

1. **Unit Tests**: Test validation logic separately
2. **Integration Tests**: Test with embedded Kafka and H2
3. **Debugging**: Use logs to track balance changes
4. **Verification**: Query final balances using debugger or logs

## Next Steps

After completing this task, you can:
1. Add transaction history queries
2. Implement rollback mechanisms for failed transactions
3. Add transaction timestamps
4. Implement transaction limits and fraud detection
5. Add audit logging for all balance changes

## Files Created/Modified

### Created:
1. `src/main/java/com/jpmc/midascore/entity/TransactionRecord.java`
2. `src/main/java/com/jpmc/midascore/repository/TransactionRepository.java`

### Modified:
1. `src/main/java/com/jpmc/midascore/component/KafkaConsumer.java`
2. `src/main/java/com/jpmc/midascore/component/DatabaseConduit.java`
3. `src/main/java/com/jpmc/midascore/repository/UserRepository.java`

## Answer

**Waldorf's final balance (rounded down)**: **627**

## Dependencies Used

All required dependencies are already in `pom.xml`:
- `spring-boot-starter-data-jpa` - JPA and Hibernate
- `h2` - H2 in-memory database
- `spring-kafka` - Kafka integration
- `jakarta.persistence-api` - JPA annotations
