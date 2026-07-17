package com.banking.fraud_detection_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;

    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeInitiatedTransaction(@Payload Map<String,Object> payload){
        log.info("Transaction received for fraud detection: {}",payload.get("transactionId"));
        try{
            fraudDetectionService.checkTransaction(payload);
        }
        catch (Exception e){
            log.error("Error initiating transaction:{}", e.getMessage());
        }
    }

}
