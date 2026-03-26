// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;

public class TrainingRecord extends AuditableEntity {
    private Employee employee;
    private String courseName;
    private String provider;
    private long completionDate;
    private int score;
    private boolean certified;
    private double cost;

    public TrainingRecord(long id, Employee employee, String courseName) {
        super(id, "system");
        this.employee = employee;
        this.courseName = courseName;
        this.score = 0;
        this.certified = false;
    }

    public Employee getEmployee() { return employee; }
    public String getCourseName() { return courseName; }
    public String getProvider() { return provider; }
    public void setProvider(String provider) { this.provider = provider; }
    public long getCompletionDate() { return completionDate; }
    public void setCompletionDate(long date) { this.completionDate = date; }
    public int getScore() { return score; }
    public void setScore(int score) { this.score = score; }
    public boolean isCertified() { return certified; }
    public void setCertified(boolean cert) { this.certified = cert; }
    public double getCost() { return cost; }
    public void setCost(double cost) { this.cost = cost; }
}
