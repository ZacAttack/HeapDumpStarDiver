package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.Address;
import com.heaptest.core.CoreEnums;

public class Shipment extends AuditableEntity {
    private PurchaseOrder purchaseOrder;
    private String trackingNumber;
    private Address origin;
    private Address destination;
    private double weight;
    private CoreEnums.Status status;
    private String carrier;

    public Shipment(long id, PurchaseOrder po, String trackingNumber) {
        super(id, "system");
        this.purchaseOrder = po;
        this.trackingNumber = trackingNumber;
        this.status = CoreEnums.Status.PENDING;
    }

    public PurchaseOrder getPurchaseOrder() { return purchaseOrder; }
    public String getTrackingNumber() { return trackingNumber; }
    public Address getOrigin() { return origin; }
    public void setOrigin(Address origin) { this.origin = origin; }
    public Address getDestination() { return destination; }
    public void setDestination(Address dest) { this.destination = dest; }
    public double getWeight() { return weight; }
    public void setWeight(double weight) { this.weight = weight; }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }
}
