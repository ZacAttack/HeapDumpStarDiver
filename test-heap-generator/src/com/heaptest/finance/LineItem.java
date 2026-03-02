package com.heaptest.finance;

import com.heaptest.core.BaseEntity;

public class LineItem extends BaseEntity {
    private String description;
    private int quantity;
    private double unitPrice;
    private double taxRate;
    private String category;

    public LineItem(long id, String description, int quantity, double unitPrice) {
        super(id);
        this.description = description;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.taxRate = 0.0;
    }

    public String getDescription() { return description; }
    public int getQuantity() { return quantity; }
    public double getUnitPrice() { return unitPrice; }
    public double getTaxRate() { return taxRate; }
    public void setTaxRate(double rate) { this.taxRate = rate; }
    public double getTotal() { return quantity * unitPrice * (1 + taxRate); }
    public String getCategory() { return category; }
    public void setCategory(String cat) { this.category = cat; }
}
