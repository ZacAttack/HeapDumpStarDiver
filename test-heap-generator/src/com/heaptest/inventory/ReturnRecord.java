// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;

public class ReturnRecord extends AuditableEntity {
    private Product product;
    private String reason;
    private int quantity;
    private double refundAmount;
    private boolean processed;
    private String condition; // NEW, USED, DAMAGED

    public ReturnRecord(long id, Product product, String reason, int quantity) {
        super(id, "system");
        this.product = product;
        this.reason = reason;
        this.quantity = quantity;
        this.refundAmount = product.getPrice() * quantity;
        this.processed = false;
    }

    public Product getProduct() { return product; }
    public String getReason() { return reason; }
    public int getQuantity() { return quantity; }
    public double getRefundAmount() { return refundAmount; }
    public void setRefundAmount(double amount) { this.refundAmount = amount; }
    public boolean isProcessed() { return processed; }
    public void setProcessed(boolean p) { this.processed = p; }
    public String getCondition() { return condition; }
    public void setCondition(String c) { this.condition = c; }
}
