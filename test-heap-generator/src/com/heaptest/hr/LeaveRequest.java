// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;

public class LeaveRequest extends AuditableEntity {
    private Employee employee;
    private long startDate;
    private long endDate;
    private String type; // VACATION, SICK, PERSONAL, PARENTAL
    private boolean approved;
    private Employee approvedBy;
    private String reason;

    public LeaveRequest(long id, Employee employee, long startDate, long endDate, String type) {
        super(id, "system");
        this.employee = employee;
        this.startDate = startDate;
        this.endDate = endDate;
        this.type = type;
        this.approved = false;
    }

    public Employee getEmployee() { return employee; }
    public long getStartDate() { return startDate; }
    public long getEndDate() { return endDate; }
    public String getType() { return type; }
    public boolean isApproved() { return approved; }
    public void setApproved(boolean approved) { this.approved = approved; }
    public Employee getApprovedBy() { return approvedBy; }
    public void setApprovedBy(Employee approver) { this.approvedBy = approver; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
