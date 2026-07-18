package com.banking.notification_service.service;

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
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(@Payload Map<String,Object> payload){
        log.info("Otp recieved to be sent by notification service: {}",payload.get("transactionId"));

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");
            String otp = (String) payload.get("otp");

            sendAlert(accountNumber,
                    "Transaction verification required",
                    String.format(
                            "Suspicious activity detected on your account. " +
                            "Reason: %s "+
                            "A transaction %s has pending verification "+
                            "Your OTP is %s, valid for 5 minutes "+
                            "If this was not you, ignore this message"
                    )
            );
        }
        catch (Exception e){
            log.error("Error sending OTP notification: {}",e.getMessage());
        }
    }

    public void sendAlert(String accountNumber, String subject, String message){

    }
}
