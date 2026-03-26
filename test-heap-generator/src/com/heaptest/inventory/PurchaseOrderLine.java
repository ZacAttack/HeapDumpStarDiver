// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.BaseEntity;

public class PurchaseOrderLine extends BaseEntity {
    private Product product;
    private int quantity;
    private double unitPrice;

    public PurchaseOrderLine(long id, Product product, int quantity, double unitPrice) {
        super(id);
        this.product = product;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }

    public Product getProduct() { return product; }
    public int getQuantity() { return quantity; }
    public double getUnitPrice() { return unitPrice; }
    public double getLineTotal() { return quantity * unitPrice; }
}
