package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Team extends AuditableEntity {
    private String name;
    private List<Object> members; // Employee refs
    private Object lead; // Employee ref
    private List<Object> projects; // Project refs
    private Department department;

    public Team(long id, String name) {
        super(id, "system");
        this.name = name;
        this.members = new ArrayList<>();
        this.projects = new ArrayList<>();
    }

    public String getName() { return name; }
    public List<Object> getMembers() { return members; }
    public Object getLead() { return lead; }
    public void setLead(Object lead) { this.lead = lead; }
    public List<Object> getProjects() { return projects; }
    public Department getDepartment() { return department; }
    public void setDepartment(Department d) { this.department = d; }
    public void addMember(Object e) { members.add(e); }
    public void addProject(Object p) { projects.add(p); }
}
