package com.banking.acount_service.repository;

import com.banking.acount_service.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account,String> {

    boolean existsByEmail(String email);

    boolean existsByAccountNumber(String accountNumber);

    Optional<Account> findByAccountNumber(String accountNumber);
}
