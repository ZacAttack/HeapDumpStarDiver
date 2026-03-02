package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.CoreEnums;
import java.util.ArrayList;
import java.util.List;

public class Task extends AuditableEntity {
    private String title;
    private String description;
    private Object assignee; // hr.Employee
    private CoreEnums.Priority priority;
    private CoreEnums.Status status;
    private List<Task> subTasks; // self-reference!
    private Task parentTask; // self-reference!
    private int storyPoints;
    private Sprint sprint;
    private String type; // BUG, FEATURE, CHORE, SPIKE

    public Task(long id, String title) {
        super(id, "system");
        this.title = title;
        this.subTasks = new ArrayList<>();
        this.priority = CoreEnums.Priority.MEDIUM;
        this.status = CoreEnums.Status.PENDING;
        this.storyPoints = 0;
    }

    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public Object getAssignee() { return assignee; }
    public void setAssignee(Object assignee) { this.assignee = assignee; }
    public CoreEnums.Priority getPriority() { return priority; }
    public void setPriority(CoreEnums.Priority p) { this.priority = p; }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public List<Task> getSubTasks() { return subTasks; }
    public void addSubTask(Task t) { subTasks.add(t); t.parentTask = this; }
    public Task getParentTask() { return parentTask; }
    public int getStoryPoints() { return storyPoints; }
    public void setStoryPoints(int sp) { this.storyPoints = sp; }
    public Sprint getSprint() { return sprint; }
    public void setSprint(Sprint s) { this.sprint = s; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}
