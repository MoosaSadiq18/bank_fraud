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
                            "If this was not you, ignore this message",
                            accountNumber,reason,otp
                    )
            );
        }
        catch (Exception e){
            log.error("Error sending OTP notification: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(
                    senderAccountNumber,
                    "Debit notification",
                    String.format("%s debited from account %s", amount, senderAccountNumber)
            );

            sendAlert(
                    senderAccountNumber,
                    "Credit notification",
                    String.format("%s credited from account %s", amount, receiverAccountNumber)
            );
        }
        catch(Exception e){
            log.error("Error sending debit/credit notifications: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){
        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "Fraud notification",
                    String.format("Fraud detected from account %s with reason %s, your account has been blocked", accountNumber, reason)
            );
        }
        catch(Exception e){
            log.error("Error sending fraud notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String,Object> payload){
        try{
            String amount = payload.get("amount").toString();
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(
                    senderAccountNumber,
                    "Amount refunded",
                    String.format("Amount %s refunded to account %s with reason %s", amount,senderAccountNumber,reason)
            );
        }
        catch(Exception e){
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String,Object> payload){
        try{
            String amount = payload.get("amount").toString();
            String accountNumber = (String) payload.get("accountNumber");

            sendAlert(
                    accountNumber,
                    "Amount paid at Razorpay",
                    String.format("Amount %s paid by account %s at Razorpay", amount,accountNumber)
            );
        }
        catch(Exception e){
            log.error("Error sending Razorpay payment completion notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload){
        try{
            String amount = payload.get("amount").toString();
            String accountNumber = (String) payload.get("accountNumber");

            sendAlert(
                    accountNumber,
                    "Amount payment failed at Razorpay",
                    String.format("Amount %s payment failed by account %s at Razorpay", amount,accountNumber)
            );
        }
        catch(Exception e){
            log.error("Error sending Razorpay payment failure notification: {}", e.getMessage());
        }
    }

    public void sendAlert(String accountNumber, String subject, String message){
        log.info("---------------");
        log.info("Account: {}",accountNumber);
        log.info("Subject: {}",subject);
        log.info("Message: {}",message);
        log.info("---------------");
    }
}
