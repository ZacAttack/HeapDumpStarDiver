package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.Address;

public class Subsidiary extends AuditableEntity {
    private String name;
    private Organization parentOrg;
    private Address address;
    private double revenue;
    private int headcount;
    private String countryCode;

    public Subsidiary(long id, String name, Address address) {
        super(id, "system");
        this.name = name;
        this.address = address;
        this.revenue = 0.0;
        this.headcount = 0;
    }

    public String getName() { return name; }
    public Organization getParentOrg() { return parentOrg; }
    public void setParentOrg(Organization org) { this.parentOrg = org; }
    public Address getAddress() { return address; }
    public double getRevenue() { return revenue; }
    public void setRevenue(double revenue) { this.revenue = revenue; }
    public int getHeadcount() { return headcount; }
    public void setHeadcount(int headcount) { this.headcount = headcount; }
    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String code) { this.countryCode = code; }
}
