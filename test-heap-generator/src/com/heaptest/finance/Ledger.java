// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Ledger extends AuditableEntity {
    private String name;
    private List<LedgerEntry> entries;
    private List<Account> accounts;
    private double totalDebits;
    private double totalCredits;

    public Ledger(long id, String name) {
        super(id, "system");
        this.name = name;
        this.entries = new ArrayList<>();
        this.accounts = new ArrayList<>();
        this.totalDebits = 0.0;
        this.totalCredits = 0.0;
    }

    public String getName() { return name; }
    public List<LedgerEntry> getEntries() { return entries; }
    public void addEntry(LedgerEntry e) { entries.add(e); }
    public List<Account> getAccounts() { return accounts; }
    public void addAccount(Account a) { accounts.add(a); }
    public double getTotalDebits() { return totalDebits; }
    public double getTotalCredits() { return totalCredits; }
}
