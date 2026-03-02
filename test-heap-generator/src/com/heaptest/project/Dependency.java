package com.heaptest.project;

import com.heaptest.core.BaseEntity;

public class Dependency extends BaseEntity {
    private Task fromTask;
    private Task toTask;
    private String type; // BLOCKS, DEPENDS_ON, RELATED
    private boolean critical;
    private String notes;

    public Dependency(long id, Task from, Task to, String type) {
        super(id);
        this.fromTask = from;
        this.toTask = to;
        this.type = type;
        this.critical = false;
    }

    public Task getFromTask() { return fromTask; }
    public Task getToTask() { return toTask; }
    public String getType() { return type; }
    public boolean isCritical() { return critical; }
    public void setCritical(boolean c) { this.critical = c; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
}
