// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;

public class TaxRecord extends AuditableEntity {
    private Object employee; // hr.Employee
    private int year;
    private double totalIncome;
    private double totalTax;
    private double federalTax;
    private double stateTax;
    private boolean filed;

    public TaxRecord(long id, Object employee, int year) {
        super(id, "system");
        this.employee = employee;
        this.year = year;
        this.totalIncome = 0.0;
        this.totalTax = 0.0;
        this.filed = false;
    }

    public Object getEmployee() { return employee; }
    public int getYear() { return year; }
    public double getTotalIncome() { return totalIncome; }
    public void setTotalIncome(double income) { this.totalIncome = income; }
    public double getTotalTax() { return totalTax; }
    public void setTotalTax(double tax) { this.totalTax = tax; }
    public double getFederalTax() { return federalTax; }
    public void setFederalTax(double tax) { this.federalTax = tax; }
    public double getStateTax() { return stateTax; }
    public void setStateTax(double tax) { this.stateTax = tax; }
    public boolean isFiled() { return filed; }
    public void setFiled(boolean filed) { this.filed = filed; }
}
