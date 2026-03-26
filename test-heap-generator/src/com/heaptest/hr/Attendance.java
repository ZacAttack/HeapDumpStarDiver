// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.hr;

import com.heaptest.core.BaseEntity;

public class Attendance extends BaseEntity {
    private Employee employee;
    private long date;
    private float hoursWorked;
    private boolean overtime;
    private boolean remote;
    private String notes;

    public Attendance(long id, Employee employee, long date) {
        super(id);
        this.employee = employee;
        this.date = date;
        this.hoursWorked = 8.0f;
        this.overtime = false;
        this.remote = false;
    }

    public Employee getEmployee() { return employee; }
    public long getDate() { return date; }
    public float getHoursWorked() { return hoursWorked; }
    public void setHoursWorked(float hours) { this.hoursWorked = hours; }
    public boolean isOvertime() { return overtime; }
    public void setOvertime(boolean ot) { this.overtime = ot; }
    public boolean isRemote() { return remote; }
    public void setRemote(boolean remote) { this.remote = remote; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
}
