package com.banking.transaction_service.service;

import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String,String> redisTemplate;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private static final long OTP_EXPIRY_MINUTES = 5;

    private final TransactionService transactionService;

    private static final String TRANSACTION_OTP_GENERATION_TOPIC = "transaction.otp.generated";

    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(@Payload Map<String,Object> payload){
        log.info("Transaction received for verification: {}",payload.get("transactionId"));

        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required for transaction: {} account number: {} reason: {}",
                    transactionId, accountNumber, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction not found"));

            if(transaction.getTransactionStatus() != TransactionStatus.PROCESSING){
                log.info("Transaction: {} not processing, skipping it", transactionId);
            }

            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);
            String otpKey = "verification.opt" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            transaction.setTransactionStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {} and expires in {}",
                    transactionId, OTP_EXPIRY_MINUTES);

            Map<String,Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId",transactionId);
            otpEvent.put("accountNumber",accountNumber);
            otpEvent.put("otp",otp);
            otpEvent.put("reason",reason);
            otpEvent.put("amount",payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATION_TOPIC,transactionId,otpEvent);

        }
        catch(Exception e){
            log.error("Error handling verification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeCleanFraudCheck(@Payload Map<String,Object> payload){
        try {
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId)
        }
        catch(Exception e){
            log.error("Error clean transaction completion: {}",e.getMessage());
        }
    }

}
