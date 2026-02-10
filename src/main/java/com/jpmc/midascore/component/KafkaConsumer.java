package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    public KafkaConsumer(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
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

        // Get incentive from external API
        Incentive incentive = incentiveService.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();

        // Update balances
        // Sender: subtract transaction amount only (NOT the incentive)
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        
        // Recipient: add transaction amount PLUS incentive
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Save updated users
        databaseConduit.updateUser(sender);
        databaseConduit.updateUser(recipient);

        // Create and save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(
            sender, recipient, transaction.getAmount(), incentiveAmount
        );
        databaseConduit.saveTransaction(transactionRecord);

        logger.info("Updated balances - Sender: {} ({}), Recipient: {} ({}), Incentive: {}",
                   sender.getName(), sender.getBalance(),
                   recipient.getName(), recipient.getBalance(),
                   incentiveAmount);
    }
}
