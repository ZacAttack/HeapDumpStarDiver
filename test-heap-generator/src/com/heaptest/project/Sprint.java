package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Sprint extends AuditableEntity {
    private String name;
    private Project project;
    private List<Task> tasks;
    private long startDate;
    private long endDate;
    private int velocity;
    private boolean completed;

    public Sprint(long id, String name, Project project) {
        super(id, "system");
        this.name = name;
        this.project = project;
        this.tasks = new ArrayList<>();
        this.startDate = System.currentTimeMillis();
        this.endDate = startDate + 14L * 24 * 60 * 60 * 1000;
        this.velocity = 0;
        this.completed = false;
    }

    public String getName() { return name; }
    public Project getProject() { return project; }
    public List<Task> getTasks() { return tasks; }
    public void addTask(Task t) { tasks.add(t); t.setSprint(this); }
    public long getStartDate() { return startDate; }
    public long getEndDate() { return endDate; }
    public int getVelocity() { return velocity; }
    public void setVelocity(int v) { this.velocity = v; }
    public boolean isCompleted() { return completed; }
    public void setCompleted(boolean c) { this.completed = c; }
}
