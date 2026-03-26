// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class CostCenter extends AuditableEntity {
    private String code;
    private String name;
    private double[] monthlySpending;
    private List<Department> departments;
    private double annualBudget;

    public CostCenter(long id, String code, String name) {
        super(id, "system");
        this.code = code;
        this.name = name;
        this.monthlySpending = new double[12];
        this.departments = new ArrayList<>();
        this.annualBudget = 0.0;
    }

    public String getCode() { return code; }
    public String getName() { return name; }
    public double[] getMonthlySpending() { return monthlySpending; }
    public void setMonthlySpending(int month, double amount) { monthlySpending[month] = amount; }
    public List<Department> getDepartments() { return departments; }
    public void addDepartment(Department d) { departments.add(d); }
    public double getAnnualBudget() { return annualBudget; }
    public void setAnnualBudget(double budget) { this.annualBudget = budget; }
}
