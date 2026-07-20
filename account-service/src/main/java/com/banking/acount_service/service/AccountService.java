package com.banking.acount_service.service;

import com.banking.acount_service.dto.AccountResponse;
import com.banking.acount_service.dto.CreateAccountRequest;
import com.banking.acount_service.entity.Account;
import com.banking.acount_service.entity.AccountStatus;
import com.banking.acount_service.entity.AccountType;
import com.banking.acount_service.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request){
        log.info("Creating account for: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account with this email already exists: " + request.getEmail());
        }
        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.CURRENT
                ? new BigDecimal("100000")
                : new BigDecimal("500000")
        );

        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}",savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse response = new AccountResponse();

        response.setId(account.getId());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setAccountNumber(account.getAccountNumber());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setAccountStatus(account.getAccountStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());

        return response;
    }

    private String generateAccountNumber(){
        String accountNumber;

        do{
            Long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d",number);
        }
        while(accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    public AccountResponse getAccount(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        return account.getBalance();
    }

    public void blockAccount(String accountNumber){
        log.info("Blocking account: {}",accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}",accountNumber);
    }

    //will be called by the transaction service
    public void deductBalance(String accountNumber, BigDecimal amount){
        log.info("Deducting {} from account number: {}",amount,accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account is not active");
        }
        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient funds");
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Deducted {} from account number: {} with new balance {}",amount,accountNumber,account.getBalance());
    }

    //called by transaction service via kafka
    public void creditBalance(String accountNumber, BigDecimal amount){
        log.info("Crediting {} to account number: {}",amount,accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account is not active");
        }

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Credited {} to account number: {} with new balance {}",amount,accountNumber,account.getBalance());
    }
}
