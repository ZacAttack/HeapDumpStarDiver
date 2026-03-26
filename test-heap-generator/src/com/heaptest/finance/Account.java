// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Account extends AuditableEntity {
    private String accountNumber;
    private Object owner; // hr.Employee
    private double balance;
    private String type; // CHECKING, SAVINGS, EXPENSE, REVENUE
    private List<Transaction> transactions;
    private boolean active;
    private Currency currency;

    public Account(long id, String accountNumber, String type) {
        super(id, "system");
        this.accountNumber = accountNumber;
        this.type = type;
        this.balance = 0.0;
        this.transactions = new ArrayList<>();
        this.active = true;
    }

    public String getAccountNumber() { return accountNumber; }
    public Object getOwner() { return owner; }
    public void setOwner(Object owner) { this.owner = owner; }
    public double getBalance() { return balance; }
    public void setBalance(double balance) { this.balance = balance; }
    public String getType() { return type; }
    public List<Transaction> getTransactions() { return transactions; }
    public void addTransaction(Transaction t) { transactions.add(t); balance += t.getAmount(); }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public Currency getCurrency() { return currency; }
    public void setCurrency(Currency c) { this.currency = c; }
}
