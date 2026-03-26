// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.finance;

import com.heaptest.core.BaseEntity;

public class Transaction extends BaseEntity {
    private Account fromAccount;
    private Account toAccount;
    private double amount;
    private Currency currency;
    private long timestamp;
    private String description;
    private String referenceNumber;
    private boolean reconciled;

    public Transaction(long id, Account from, Account to, double amount) {
        super(id);
        this.fromAccount = from;
        this.toAccount = to;
        this.amount = amount;
        this.timestamp = System.currentTimeMillis();
        this.reconciled = false;
    }

    public Account getFromAccount() { return fromAccount; }
    public Account getToAccount() { return toAccount; }
    public double getAmount() { return amount; }
    public Currency getCurrency() { return currency; }
    public void setCurrency(Currency c) { this.currency = c; }
    public long getTimestamp() { return timestamp; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public String getReferenceNumber() { return referenceNumber; }
    public void setReferenceNumber(String ref) { this.referenceNumber = ref; }
    public boolean isReconciled() { return reconciled; }
    public void setReconciled(boolean r) { this.reconciled = r; }
}
