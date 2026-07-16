package com.banking.transaction_service.controller;

import com.banking.transaction_service.dto.TransactionRequest;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.service.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/transactions")
@Slf4j
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(@Valid @RequestBody TransactionRequest request){
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(transactionService.transfer(request);
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionResponse> getTransaction(@PathVariable String transactionId){
        return ResponseEntity.ok(transactionService.getTransaction(transactionId));
    }

    @GetMapping("/account/{accountNumber}")
    public ResponseEntity<TransactionResponse> getTransactionHistory(@PathVariable String accountNumber){
        return ResponseEntity.ok(transactionService.getTransactionHistory(accountNumber));
    }

    @PostMapping("/{transactionId}/verify")
    public ResponseEntity<TransactionResponse> verifyOtp(@PathVariable String transactionId,
                                                         @RequestParam String otp)
    {
        log.info("OTP verification request for transaction: {}",transactionId);
        return ResponseEntity.ok(transactionService.verifyOtp(transactionId,otp));
    }
}
