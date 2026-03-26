// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Milestone extends AuditableEntity {
    private String name;
    private Project project;
    private long targetDate;
    private List<Task> tasks;
    private boolean completed;
    private String description;

    public Milestone(long id, String name, Project project, long targetDate) {
        super(id, "system");
        this.name = name;
        this.project = project;
        this.targetDate = targetDate;
        this.tasks = new ArrayList<>();
        this.completed = false;
    }

    public String getName() { return name; }
    public Project getProject() { return project; }
    public long getTargetDate() { return targetDate; }
    public List<Task> getTasks() { return tasks; }
    public void addTask(Task t) { tasks.add(t); }
    public boolean isCompleted() { return completed; }
    public void setCompleted(boolean c) { this.completed = c; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
}
