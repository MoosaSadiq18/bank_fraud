package com.banking.acount_service.controller;

import com.banking.acount_service.dto.AccountResponse;
import com.banking.acount_service.dto.CreateAccountRequest;
import com.banking.acount_service.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    private ResponseEntity<AccountResponse> createAccount(@Valid @RequestBody CreateAccountRequest request){
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(accountService.createAccount(request));
    }

    @GetMapping("/{accountNumber}")
    private ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    private ResponseEntity<String> blockAccount(@PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");
    }

    @GetMapping("/{accountNumber}/getBalance")
    public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/deduct")
    private ResponseEntity<String> deductBalance(@PathVariable String accountNumber,
                                                     @RequestParam BigDecimal amount){
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Balance deducted successfully");
    }

    @PutMapping("/{accountNumber}/credit")
    private ResponseEntity<String> creditBalance(@PathVariable String accountNumber,
                                                 @RequestParam BigDecimal amount){
        accountService.creditBalance(accountNumber, amount);
        return ResponseEntity.ok("Balance credited successfully");
    }

}
