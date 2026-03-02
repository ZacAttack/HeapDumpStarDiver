package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Invoice extends AuditableEntity {
    private String invoiceNumber;
    private String vendor;
    private List<LineItem> lineItems;
    private double total;
    private boolean paid;
    private long dueDate;
    private PaymentMethod paymentMethod;

    public Invoice(long id, String invoiceNumber, String vendor) {
        super(id, "system");
        this.invoiceNumber = invoiceNumber;
        this.vendor = vendor;
        this.lineItems = new ArrayList<>();
        this.total = 0.0;
        this.paid = false;
        this.dueDate = System.currentTimeMillis() + 30L * 24 * 60 * 60 * 1000;
    }

    public String getInvoiceNumber() { return invoiceNumber; }
    public String getVendor() { return vendor; }
    public List<LineItem> getLineItems() { return lineItems; }
    public void addLineItem(LineItem li) { lineItems.add(li); total += li.getTotal(); }
    public double getTotal() { return total; }
    public boolean isPaid() { return paid; }
    public void setPaid(boolean paid) { this.paid = paid; }
    public long getDueDate() { return dueDate; }
    public void setDueDate(long date) { this.dueDate = date; }
    public PaymentMethod getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(PaymentMethod pm) { this.paymentMethod = pm; }
}
