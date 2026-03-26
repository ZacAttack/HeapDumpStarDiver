// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Department extends AuditableEntity {
    private String name;
    private String code;
    private List<Team> teams;
    private List<Object> employees; // Will hold Employee refs
    private Object budget; // Will hold Budget ref
    private CostCenter costCenter;
    private Organization organization;

    public Department(long id, String name, String code) {
        super(id, "system");
        this.name = name;
        this.code = code;
        this.teams = new ArrayList<>();
        this.employees = new ArrayList<>();
    }

    public String getName() { return name; }
    public String getCode() { return code; }
    public List<Team> getTeams() { return teams; }
    public List<Object> getEmployees() { return employees; }
    public Object getBudget() { return budget; }
    public void setBudget(Object budget) { this.budget = budget; }
    public CostCenter getCostCenter() { return costCenter; }
    public void setCostCenter(CostCenter cc) { this.costCenter = cc; }
    public Organization getOrganization() { return organization; }
    public void setOrganization(Organization org) { this.organization = org; }
    public void addTeam(Team t) { teams.add(t); }
    public void addEmployee(Object e) { employees.add(e); }
}
