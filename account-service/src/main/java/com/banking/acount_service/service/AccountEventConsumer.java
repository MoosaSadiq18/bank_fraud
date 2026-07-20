package com.banking.acount_service.service;

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
public class AccountEventConsumer {

    private final AccountService accountService;

    @KafkaListener(topics = "transaction.completed")
    public void consumeCompletedTransaction(@Payload Map<String,Object> payload){

        try {
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            Object val = payload.get("amount");
            BigDecimal amount = new BigDecimal(val.toString());

            log.info("Crediting account number {} amount {}",receiverAccountNumber,amount);
            accountService.creditBalance(receiverAccountNumber,amount);

        }
        catch (Exception e){
            log.error("Error crediting acount:{}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected: blocking account: {}",accountNumber);
            accountService.blockAccount(accountNumber);
        }
        catch (Exception e){
            log.error("Error Blocking account:{}", e.getMessage());
        }
    }
}
