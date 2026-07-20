package com.banking.transaction_service.service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionRequest;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.TransactionInitiatedEvent;
import com.banking.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    public TransactionResponse transfer(TransactionRequest request){
        log.info("Transferring: {} -> {} amount:{}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setTransactionType(TransactionType.TRANSFER);
        transaction.setTransactionStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        //this event will be consumed by the fraud detection service
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("TransactionInitiatedEvent published: {}", savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId){
        return mapToResponse(transactionRepository.findById(transactionId)
                .orElseThrow(()-> new RuntimeException("Transaction not found " + transactionId)));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found"));

        if(transaction.getTransactionStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction not processing: {}",transactionId);
            return;
        }

        completeTransaction(transaction);
    }

    public TransactionResponse verifyOtp(String transactionId, String otp){
        log.info("Got input fot the OTP verification for transaction: {}",transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found"));

        String otpKey = "verification.opt" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp==null){
            log.warn("OTP expired: for transaction id: {}",transactionId);
            compensateTransaction(transaction, "OTP expired, amount refunded");
            return mapToResponse(transaction);
        }

        if(!otp.equals(storedOtp)){
            log.warn("OTP not matched, blocking account and refunding amount: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensateTransaction(transaction,"Wrong otp received");
        }

        log.info("OTP verified, completing the transaction: {}",transactionId);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    public void completeTransaction(Transaction transaction){
        transaction.setTransactionStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        Map<String,Object> completionEvent = new HashMap<>();
        completionEvent.put("transactionId",transaction.getId());
        completionEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        completionEvent.put("receiverAccountNumber",transaction.getReceiverAccountNumber());
        completionEvent.put("description",transaction.getDescription());
        completionEvent.put("amount",transaction.getAmount());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),completionEvent);
    }

    public void compensateTransaction(Transaction transaction, String reason){
        log.warn("Refunding back: {} amount: {}",transaction.getId(),transaction.getAmount());
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(),transaction.getAmount());

        transaction.setTransactionStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason);
        transactionRepository.save(transaction);

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("reason",reason);
        refundEvent.put("amount",transaction.getAmount());

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("Amount: {} refunded back at transaction: {}",
                transaction.getAmount(),transaction.getId());
    }

    public void blockAccountAndCompensateTransaction(Transaction transaction, String reason){
        log.warn("Blocking account: {} and Refunding back: {} amount: {}",
                transaction.getSenderAccountNumber(),transaction.getId(),transaction.getAmount());

        Map<String,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getId(),fraudEvent);
        log.warn("fraud.detect topic published to block the account: {}", transaction.getSenderAccountNumber());

        compensateTransaction(transaction,reason);
    }

    public TransactionResponse mapToResponse(Transaction request){
        TransactionResponse response = new TransactionResponse();
        response.setId(request.getId());
        response.setSenderAccountNumber(request.getSenderAccountNumber());
        response.setReceiverAccountNumber(request.getReceiverAccountNumber());
        response.setAmount(request.getAmount());
        response.setCreatedAt(request.getCreatedAt());
        response.setCompletedAt(request.getCompletedAt());
        response.setDescription(request.getDescription());
        response.setFailureReason(request.getFailureReason());
        response.setReferenceNumber(request.getReferenceNumber());
        response.setTransactionType(request.getTransactionType());
        response.setTransactionStatus(request.getTransactionStatus());

        return response;
    }

}
