package com.heaptest.finance;

import com.heaptest.core.BaseEntity;

public class LedgerEntry extends BaseEntity {
    private Account account;
    private double amount;
    private String type; // DEBIT, CREDIT
    private String description;
    private double balanceAfter;
    private long entryDate;

    public LedgerEntry(long id, Account account, double amount, String type) {
        super(id);
        this.account = account;
        this.amount = amount;
        this.type = type;
        this.balanceAfter = 0.0;
        this.entryDate = System.currentTimeMillis();
    }

    public Account getAccount() { return account; }
    public double getAmount() { return amount; }
    public String getType() { return type; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public double getBalanceAfter() { return balanceAfter; }
    public void setBalanceAfter(double bal) { this.balanceAfter = bal; }
    public long getEntryDate() { return entryDate; }
}
