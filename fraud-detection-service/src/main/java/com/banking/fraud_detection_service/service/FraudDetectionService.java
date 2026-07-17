package com.banking.fraud_detection_service.service;

import com.banking.fraud_detection_service.client.AccountServiceClient;
import com.banking.fraud_detection_service.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionsPerMinute;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String,Object> payload){
        String transactionId = (String) payload.get("transactionId");
        String senderAccountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = (BigDecimal) payload.get("amount");

        BigDecimal senderBalance = accountServiceClient.getBalance(senderAccountNumber);
        log.info("Checking transaction: {} for account: {} amount: {} balance: {}",
                transactionId,senderAccountNumber,amount,senderBalance);

        FraudCheckResult result = performFraudCheck(senderAccountNumber,amount,senderBalance);

        if(result.isFraud()){
            log.info("Suspicous activity detected for acount: {}" + "with reason: {} - requesting OTP verification",
                    senderAccountNumber, result.getReason());

            Map<String,Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId",transactionId);
            verificationEvent.put("senderAccountNumber",senderAccountNumber);
            verificationEvent.put("amount",amount);
            verificationEvent.put("reason",result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC,transactionId,verificationEvent);
            log.info("VerificationRequiredEvent published: {}", transactionId);
        }
        else{
            log.info("Transaction: {} is clean", transactionId);

            Map<String,Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId",transactionId);
            transactionCleanEvent.put("isFraud",false);
            transactionCleanEvent.put("reason",null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC,transactionId,transactionCleanEvent);
            log.info("FraudCheckCleanResultEvent published: {}", transactionId);
        }
    }

    private FraudCheckResult performFraudCheck(String accountNumber,
                                               BigDecimal amount,
                                               BigDecimal senderBalance)
    {
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true,"Too many transactions in 60 seconds");
        }
        if(isAmountSuspicious(accountNumber,amount)){
            return new FraudCheckResult(true,"Amount greater than the average transactions");
        }
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFailed(senderBalance,amount)){
            return new FraudCheckResult(true,"Transaction exceeded 90% of account balance");
        }

        return new FraudCheckResult(false,null);
    }

    private boolean isVelocityExceeded(String accountNumber){
        String key = "fraud.velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count!=null && count==1){
            redisTemplate.expire(key,60, TimeUnit.SECONDS);
        }

        log.info("Velocity check: account: {} count: {}/{}",
                accountNumber,count,maxTransactionsPerMinute);

        return count!=null && count>maxTransactionsPerMinute;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount){
        return true;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount){
        return true;
    }
}
