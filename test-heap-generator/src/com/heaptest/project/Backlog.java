// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.LinkedList;

public class Backlog extends AuditableEntity {
    private Project project;
    private LinkedList<Task> items;
    private boolean prioritized;
    private int totalPoints;

    public Backlog(long id, Project project) {
        super(id, "system");
        this.project = project;
        this.items = new LinkedList<>();
        this.prioritized = false;
        this.totalPoints = 0;
    }

    public Project getProject() { return project; }
    public LinkedList<Task> getItems() { return items; }
    public void addItem(Task t) { items.add(t); totalPoints += t.getStoryPoints(); }
    public void addItemFirst(Task t) { items.addFirst(t); totalPoints += t.getStoryPoints(); }
    public boolean isPrioritized() { return prioritized; }
    public void setPrioritized(boolean p) { this.prioritized = p; }
    public int getTotalPoints() { return totalPoints; }
}
