// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.CoreEnums;
import java.util.ArrayList;
import java.util.List;

public class PurchaseOrder extends AuditableEntity {
    private Supplier supplier;
    private List<PurchaseOrderLine> lines;
    private double total;
    private CoreEnums.Status status;
    private long orderDate;
    private long expectedDelivery;

    public PurchaseOrder(long id, Supplier supplier) {
        super(id, "system");
        this.supplier = supplier;
        this.lines = new ArrayList<>();
        this.total = 0.0;
        this.status = CoreEnums.Status.PENDING;
        this.orderDate = System.currentTimeMillis();
    }

    public Supplier getSupplier() { return supplier; }
    public List<PurchaseOrderLine> getLines() { return lines; }
    public void addLine(PurchaseOrderLine line) { lines.add(line); total += line.getLineTotal(); }
    public double getTotal() { return total; }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public long getOrderDate() { return orderDate; }
    public long getExpectedDelivery() { return expectedDelivery; }
    public void setExpectedDelivery(long date) { this.expectedDelivery = date; }
}
