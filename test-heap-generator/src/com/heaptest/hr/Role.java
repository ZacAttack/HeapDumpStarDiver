package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Role extends AuditableEntity {
    private String title;
    private int level;
    private List<String> permissions;
    private String description;
    private double minSalary;
    private double maxSalary;

    public Role(long id, String title, int level) {
        super(id, "system");
        this.title = title;
        this.level = level;
        this.permissions = new ArrayList<>();
    }

    public String getTitle() { return title; }
    public int getLevel() { return level; }
    public List<String> getPermissions() { return permissions; }
    public void addPermission(String perm) { permissions.add(perm); }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public double getMinSalary() { return minSalary; }
    public void setMinSalary(double s) { this.minSalary = s; }
    public double getMaxSalary() { return maxSalary; }
    public void setMaxSalary(double s) { this.maxSalary = s; }
}
