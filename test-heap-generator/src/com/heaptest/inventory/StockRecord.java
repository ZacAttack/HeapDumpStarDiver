// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.BaseEntity;

public class StockRecord extends BaseEntity {
    private Product product;
    private Warehouse warehouse;
    private int quantity;
    private int minQuantity;
    private long lastRestocked;
    private String location; // aisle-shelf-bin

    public StockRecord(long id, Product product, Warehouse warehouse, int quantity) {
        super(id);
        this.product = product;
        this.warehouse = warehouse;
        this.quantity = quantity;
        this.minQuantity = 10;
        this.lastRestocked = System.currentTimeMillis();
    }

    public Product getProduct() { return product; }
    public Warehouse getWarehouse() { return warehouse; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int qty) { this.quantity = qty; }
    public int getMinQuantity() { return minQuantity; }
    public void setMinQuantity(int min) { this.minQuantity = min; }
    public long getLastRestocked() { return lastRestocked; }
    public void setLastRestocked(long date) { this.lastRestocked = date; }
    public boolean needsRestock() { return quantity < minQuantity; }
    public String getLocation() { return location; }
    public void setLocation(String loc) { this.location = loc; }
}
