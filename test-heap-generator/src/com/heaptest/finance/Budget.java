// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Budget extends AuditableEntity {
    private String name;
    private double[] monthlyAllocations;
    private Object department; // org.Department
    private List<LineItem> lineItems;
    private double totalBudget;
    private int fiscalYear;

    public Budget(long id, String name, int fiscalYear) {
        super(id, "system");
        this.name = name;
        this.fiscalYear = fiscalYear;
        this.monthlyAllocations = new double[12];
        this.lineItems = new ArrayList<>();
        this.totalBudget = 0.0;
    }

    public String getName() { return name; }
    public double[] getMonthlyAllocations() { return monthlyAllocations; }
    public void setMonthlyAllocation(int month, double amount) { monthlyAllocations[month] = amount; }
    public Object getDepartment() { return department; }
    public void setDepartment(Object dept) { this.department = dept; }
    public List<LineItem> getLineItems() { return lineItems; }
    public void addLineItem(LineItem li) { lineItems.add(li); }
    public double getTotalBudget() { return totalBudget; }
    public void setTotalBudget(double total) { this.totalBudget = total; }
    public int getFiscalYear() { return fiscalYear; }
}
