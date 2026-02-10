# Task 5 Solution: Exposing a REST API for Balance Queries

## Overview
This task involves creating a REST API endpoint within Midas Core to allow users to query their account balances. The implementation demonstrates how to expose REST endpoints in a Spring Boot application that already has other components like Kafka listeners and database layers.

## Solution Summary

**Test Output:**
```
---begin output ---
Balance {amount=0.0}
Balance {amount=1326.98}
Balance {amount=2567.52}
Balance {amount=2740.33}
Balance {amount=140.96999}
Balance {amount=10.419973}
Balance {amount=845.49005}
Balance {amount=657.49}
Balance {amount=99.189995}
Balance {amount=3434.0002}
Balance {amount=2157.1902}
Balance {amount=779421.3}
Balance {amount=0.0}
---end output ---
```

---

## Implementation Steps

### Step 1: Create the REST Controller

**File:** `src/main/java/com/jpmc/midascore/controller/BalanceController.java`

**Purpose:** Create a REST controller to expose the `/balance` endpoint.

**Implementation:**
```java
package com.jpmc.midascore.controller;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {
    
    private final DatabaseConduit databaseConduit;

    public BalanceController(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        UserRecord user = databaseConduit.findUserById(userId);
        
        if (user != null) {
            return new Balance(user.getBalance());
        } else {
            return new Balance(0);
        }
    }
}
```

**Key Points:**
- `@RestController` annotation makes this a REST controller
- `@GetMapping("/balance")` maps GET requests to the `/balance` endpoint
- `@RequestParam Long userId` extracts the userId from the query parameter
- Returns a `Balance` object that Spring automatically serializes to JSON
- Returns balance of 0 if user doesn't exist

---

### Step 2: Configure Server Port

**File:** `application.yml`

**Purpose:** Configure the application to run on port 33400.

**Changes Made:**
```yaml
server:
  port: 33400

general:
  kafka-topic: trader-updates

spring:
  # ... rest of configuration
```

**Key Points:**
- Added `server.port: 33400` at the top of the configuration
- This ensures the REST API is accessible on the correct port
- The application now runs both the Kafka listener and REST API on port 33400

---

## Running the Solution

### Prerequisites
1. **Start the Incentive API Service:**
   ```bash
   java -jar services/transaction-incentive-api.jar
   ```
   - Must be running on port 8080

2. **Ensure Midas Core is configured correctly:**
   - Port 33400 is set in application.yml
   - BalanceController is created

### Execute the Tests
```bash
mvn -Dtest=TaskFiveTests test
```

### Test the Endpoint Manually
You can also test the endpoint using curl or a browser:
```bash
curl "http://localhost:33400/balance?userId=1"
```

Expected response:
```json
{
  "amount": 1326.98
}
```

---

## Architecture Concepts

### Adding Functionality to Existing Components

**The Decision:**
- Add the balance query endpoint directly to Midas Core
- Alternative: Create a separate microservice for balance queries

**Reasoning:**
- Midas Core currently has a single focused task: handle incoming transactions
- Adding a REST controller slightly muddies this purpose
- However, the decreased development time and deployment burden outweigh the architectural concerns
- Spring makes it easy to add REST controllers to existing applications
- The feature is relatively minor and doesn't warrant a new component

**When to Extract:**
If the behavior evolves and becomes more complex, it can be extracted later:
- Multiple endpoints need to be added
- Complex query logic is required
- The balance service needs independent scaling
- Clear separation of concerns becomes more valuable

### REST Controller Integration

**How It Works:**
- The REST controller runs alongside the Kafka listener in the same Spring application
- Both components share the same database layer (DatabaseConduit)
- Spring Boot automatically configures the embedded web server (Tomcat)
- The application now serves both:
  - Asynchronous message processing (Kafka)
  - Synchronous HTTP requests (REST API)

**Benefits:**
- Single deployment unit
- Shared database connection pool
- Consistent transaction handling
- Simplified infrastructure

---

## Key Takeaways

1. **Spring REST Controllers:** Use `@RestController` and `@GetMapping` to expose REST endpoints with minimal configuration

2. **Request Parameters:** `@RequestParam` extracts query parameters from the URL

3. **JSON Serialization:** Spring automatically serializes Java objects to JSON responses

4. **Port Configuration:** Use `server.port` in application.yml to configure the server port

5. **Architectural Balance:** Good architecture balances elegance with practical concerns like development time and deployment complexity

6. **Component Integration:** REST controllers can coexist with other Spring components (Kafka listeners, database layers) in the same application

7. **Graceful Degradation:** Return sensible defaults (balance of 0) when data doesn't exist

---

## Testing Notes

The TaskFiveTests:
- Populates users in the database
- Sends transactions via Kafka
- Queries balances for user IDs 0-12 via the REST API
- Verifies the endpoint returns correct JSON-serialized Balance objects
- Uses `BalanceQuerier` utility to make HTTP GET requests to the endpoint

The test output shows balances for 13 users (IDs 0-12), with user ID 0 and 12 having balance 0 (non-existent users).

---

## Dependencies Used

From `pom.xml`:
- `spring-boot-starter-web` - Provides REST controller support and embedded Tomcat
- `spring-boot-starter-data-jpa` - Database persistence
- `spring-kafka` - Kafka message consumption

No additional dependencies were required for this task.
