package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Division extends AuditableEntity {
    private String name;
    private List<Department> departments;
    private Object head; // Employee ref
    private Organization organization;

    public Division(long id, String name) {
        super(id, "system");
        this.name = name;
        this.departments = new ArrayList<>();
    }

    public String getName() { return name; }
    public List<Department> getDepartments() { return departments; }
    public Object getHead() { return head; }
    public void setHead(Object head) { this.head = head; }
    public Organization getOrganization() { return organization; }
    public void setOrganization(Organization org) { this.organization = org; }
    public void addDepartment(Department d) { departments.add(d); }
}
