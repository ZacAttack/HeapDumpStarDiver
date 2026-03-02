package com.heaptest.finance;

import com.heaptest.core.BaseEntity;

public class Currency extends BaseEntity {
    private String code;
    private String name;
    private char symbol;
    private double exchangeRate;
    private boolean active;

    public Currency(long id, String code, String name, char symbol) {
        super(id);
        this.code = code;
        this.name = name;
        this.symbol = symbol;
        this.exchangeRate = 1.0;
        this.active = true;
    }

    public String getCode() { return code; }
    public String getName() { return name; }
    public char getSymbol() { return symbol; }
    public double getExchangeRate() { return exchangeRate; }
    public void setExchangeRate(double rate) { this.exchangeRate = rate; }
    public boolean isActive() { return active; }
}
