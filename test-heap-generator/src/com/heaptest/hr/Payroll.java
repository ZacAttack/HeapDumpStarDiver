package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;
import java.util.HashMap;
import java.util.Map;

public class Payroll extends AuditableEntity {
    private Employee employee;
    private long[] monthlyGross;
    private double taxRate;
    private Map<String, Double> deductions;
    private String payFrequency;
    private String bankAccount;

    public Payroll(long id, Employee employee) {
        super(id, "system");
        this.employee = employee;
        this.monthlyGross = new long[12];
        this.taxRate = 0.25;
        this.deductions = new HashMap<>();
        this.payFrequency = "MONTHLY";
    }

    public Employee getEmployee() { return employee; }
    public long[] getMonthlyGross() { return monthlyGross; }
    public void setMonthlyGross(int month, long amount) { monthlyGross[month] = amount; }
    public double getTaxRate() { return taxRate; }
    public void setTaxRate(double rate) { this.taxRate = rate; }
    public Map<String, Double> getDeductions() { return deductions; }
    public void addDeduction(String name, double amount) { deductions.put(name, amount); }
    public String getPayFrequency() { return payFrequency; }
    public String getBankAccount() { return bankAccount; }
    public void setBankAccount(String acct) { this.bankAccount = acct; }
}
