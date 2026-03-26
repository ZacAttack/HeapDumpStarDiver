// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;

public class Benefits extends AuditableEntity {
    private Employee employee;
    private String healthPlan;
    private String dentalPlan;
    private String visionPlan;
    private double retirementContribution;
    private double employerMatch;
    private int ptoBalance;
    private int sickDays;

    public Benefits(long id, Employee employee) {
        super(id, "system");
        this.employee = employee;
        this.retirementContribution = 0.0;
        this.employerMatch = 0.0;
        this.ptoBalance = 20;
        this.sickDays = 10;
    }

    public Employee getEmployee() { return employee; }
    public String getHealthPlan() { return healthPlan; }
    public void setHealthPlan(String plan) { this.healthPlan = plan; }
    public String getDentalPlan() { return dentalPlan; }
    public void setDentalPlan(String plan) { this.dentalPlan = plan; }
    public String getVisionPlan() { return visionPlan; }
    public void setVisionPlan(String plan) { this.visionPlan = plan; }
    public double getRetirementContribution() { return retirementContribution; }
    public void setRetirementContribution(double pct) { this.retirementContribution = pct; }
    public double getEmployerMatch() { return employerMatch; }
    public void setEmployerMatch(double pct) { this.employerMatch = pct; }
    public int getPtoBalance() { return ptoBalance; }
    public void setPtoBalance(int days) { this.ptoBalance = days; }
    public int getSickDays() { return sickDays; }
}
