package com.banking.transaction_service.service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionRequest;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

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
    }
}
