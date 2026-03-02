package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;

public class PaymentMethod extends AuditableEntity {
    private String type; // CREDIT_CARD, WIRE, ACH, CHECK
    private String last4;
    private long expiryDate;
    private boolean active;
    private String billingAddress;

    public PaymentMethod(long id, String type, String last4) {
        super(id, "system");
        this.type = type;
        this.last4 = last4;
        this.active = true;
        this.expiryDate = System.currentTimeMillis() + 365L * 24 * 60 * 60 * 1000;
    }

    public String getType() { return type; }
    public String getLast4() { return last4; }
    public long getExpiryDate() { return expiryDate; }
    public void setExpiryDate(long date) { this.expiryDate = date; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public String getBillingAddress() { return billingAddress; }
    public void setBillingAddress(String addr) { this.billingAddress = addr; }
}
