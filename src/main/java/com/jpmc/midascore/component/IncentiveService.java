package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    
    private final RestTemplate restTemplate;

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive getIncentive(Transaction transaction) {
        try {
            logger.info("Requesting incentive for transaction: senderId={}, recipientId={}, amount={}", 
                       transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
            
            Incentive incentive = restTemplate.postForObject(
                INCENTIVE_API_URL, 
                transaction, 
                Incentive.class
            );
            
            if (incentive != null) {
                logger.info("Received incentive: {}", incentive.getAmount());
                return incentive;
            } else {
                logger.warn("Received null incentive response, defaulting to 0");
                return new Incentive(0);
            }
        } catch (Exception e) {
            logger.error("Error calling incentive API: {}", e.getMessage());
            // Return zero incentive on error to allow transaction to proceed
            return new Incentive(0);
        }
    }
}
