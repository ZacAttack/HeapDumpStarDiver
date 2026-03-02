package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.Address;
import java.util.ArrayList;
import java.util.List;

public class Warehouse extends AuditableEntity {
    private String name;
    private Address address;
    private int capacity;
    private List<StockRecord> stockRecords;
    private Object manager; // hr.Employee
    private int currentStock;

    public Warehouse(long id, String name, Address address, int capacity) {
        super(id, "system");
        this.name = name;
        this.address = address;
        this.capacity = capacity;
        this.stockRecords = new ArrayList<>();
        this.currentStock = 0;
    }

    public String getName() { return name; }
    public Address getAddress() { return address; }
    public int getCapacity() { return capacity; }
    public List<StockRecord> getStockRecords() { return stockRecords; }
    public void addStockRecord(StockRecord sr) { stockRecords.add(sr); }
    public Object getManager() { return manager; }
    public void setManager(Object mgr) { this.manager = mgr; }
    public int getCurrentStock() { return currentStock; }
    public void setCurrentStock(int stock) { this.currentStock = stock; }
}
