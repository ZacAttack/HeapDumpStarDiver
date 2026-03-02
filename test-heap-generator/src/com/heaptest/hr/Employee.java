package com.heaptest.hr;

import java.util.ArrayList;
import java.util.List;

public class Employee extends Person {
    private String employeeId;
    private Object department; // org.Department
    private Object team; // org.Team
    private Employee manager; // self-reference!
    private List<Employee> directReports;
    private double salary;
    private List<Object> accounts; // finance.Account
    private List<Object> projects; // project.Project
    private Role role;
    private boolean active;
    private long hireDate;

    public Employee(long id, String firstName, String lastName, String email, String employeeId) {
        super(id, firstName, lastName, email);
        this.employeeId = employeeId;
        this.directReports = new ArrayList<>();
        this.accounts = new ArrayList<>();
        this.projects = new ArrayList<>();
        this.active = true;
        this.hireDate = System.currentTimeMillis();
    }

    public String getEmployeeId() { return employeeId; }
    public Object getDepartment() { return department; }
    public void setDepartment(Object dept) { this.department = dept; }
    public Object getTeam() { return team; }
    public void setTeam(Object team) { this.team = team; }
    public Employee getManager() { return manager; }
    public void setManager(Employee mgr) { this.manager = mgr; directReports(mgr); }
    private void directReports(Employee mgr) { if (mgr != null) mgr.getDirectReports().add(this); }
    public List<Employee> getDirectReports() { return directReports; }
    public double getSalary() { return salary; }
    public void setSalary(double salary) { this.salary = salary; }
    public List<Object> getAccounts() { return accounts; }
    public void addAccount(Object acct) { accounts.add(acct); }
    public List<Object> getProjects() { return projects; }
    public void addProject(Object proj) { projects.add(proj); }
    public Role getRole() { return role; }
    public void setRole(Role role) { this.role = role; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public long getHireDate() { return hireDate; }
    public void setHireDate(long date) { this.hireDate = date; }
}
