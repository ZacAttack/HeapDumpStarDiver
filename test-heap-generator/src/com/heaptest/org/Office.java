// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.Address;
import java.util.ArrayList;
import java.util.List;

public class Office extends AuditableEntity {
    private Address address;
    private char[] buildingCode;
    private int capacity;
    private int currentOccupancy;
    private List<Department> departments;
    private String phoneNumber;

    public Office(long id, Address address, String buildingCode, int capacity) {
        super(id, "system");
        this.address = address;
        this.buildingCode = buildingCode.toCharArray();
        this.capacity = capacity;
        this.currentOccupancy = 0;
        this.departments = new ArrayList<>();
    }

    public Address getAddress() { return address; }
    public char[] getBuildingCode() { return buildingCode; }
    public int getCapacity() { return capacity; }
    public int getCurrentOccupancy() { return currentOccupancy; }
    public void setCurrentOccupancy(int n) { this.currentOccupancy = n; }
    public List<Department> getDepartments() { return departments; }
    public void addDepartment(Department d) { departments.add(d); }
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phone) { this.phoneNumber = phone; }
}
