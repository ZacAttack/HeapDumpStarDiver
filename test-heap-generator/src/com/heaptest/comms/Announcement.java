package com.heaptest.comms;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.CoreEnums;
import java.util.ArrayList;
import java.util.List;

public class Announcement extends AuditableEntity {
    private String title;
    private String body;
    private Object author; // hr.Employee
    private List<String> audience;
    private CoreEnums.Priority priority;
    private long publishDate;
    private long expiryDate;

    public Announcement(long id, String title, String body) {
        super(id, "system");
        this.title = title;
        this.body = body;
        this.audience = new ArrayList<>();
        this.priority = CoreEnums.Priority.MEDIUM;
        this.publishDate = System.currentTimeMillis();
    }

    public String getTitle() { return title; }
    public String getBody() { return body; }
    public Object getAuthor() { return author; }
    public void setAuthor(Object author) { this.author = author; }
    public List<String> getAudience() { return audience; }
    public void addAudience(String group) { audience.add(group); }
    public CoreEnums.Priority getPriority() { return priority; }
    public void setPriority(CoreEnums.Priority p) { this.priority = p; }
    public long getPublishDate() { return publishDate; }
    public long getExpiryDate() { return expiryDate; }
    public void setExpiryDate(long date) { this.expiryDate = date; }
}
