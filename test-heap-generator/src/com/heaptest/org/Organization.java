// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.TaggableEntity;
import java.util.ArrayList;
import java.util.List;

public class Organization extends TaggableEntity {
    private String name;
    private String domain;
    private List<Department> departments;
    private List<Subsidiary> subsidiaries;
    private List<OrgPolicy> policies;
    private int employeeCount;

    public Organization(long id, String name, String domain) {
        super(id, "system");
        this.name = name;
        this.domain = domain;
        this.departments = new ArrayList<>();
        this.subsidiaries = new ArrayList<>();
        this.policies = new ArrayList<>();
        this.employeeCount = 0;
    }

    public String getName() { return name; }
    public String getDomain() { return domain; }
    public List<Department> getDepartments() { return departments; }
    public List<Subsidiary> getSubsidiaries() { return subsidiaries; }
    public List<OrgPolicy> getPolicies() { return policies; }
    public int getEmployeeCount() { return employeeCount; }
    public void setEmployeeCount(int count) { this.employeeCount = count; }
    public void addDepartment(Department d) { departments.add(d); }
    public void addSubsidiary(Subsidiary s) { subsidiaries.add(s); }
    public void addPolicy(OrgPolicy p) { policies.add(p); }
}
