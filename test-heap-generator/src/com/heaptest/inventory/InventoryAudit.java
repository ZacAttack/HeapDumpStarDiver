// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;

public class InventoryAudit extends AuditableEntity {
    private Warehouse warehouse;
    private Object auditor; // hr.Employee
    private int discrepancies;
    private String notes;
    private long auditDate;
    private boolean completed;

    public InventoryAudit(long id, Warehouse warehouse) {
        super(id, "system");
        this.warehouse = warehouse;
        this.discrepancies = 0;
        this.auditDate = System.currentTimeMillis();
        this.completed = false;
    }

    public Warehouse getWarehouse() { return warehouse; }
    public Object getAuditor() { return auditor; }
    public void setAuditor(Object auditor) { this.auditor = auditor; }
    public int getDiscrepancies() { return discrepancies; }
    public void setDiscrepancies(int d) { this.discrepancies = d; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
    public long getAuditDate() { return auditDate; }
    public boolean isCompleted() { return completed; }
    public void setCompleted(boolean c) { this.completed = c; }
}
