package com.heaptest.org;

import com.heaptest.core.BaseEntity;
import java.util.ArrayList;
import java.util.List;

public class OrgEvent extends BaseEntity {
    private String title;
    private String description;
    private long date;
    private Organization organization;
    private List<Object> attendees; // Employee refs
    private String location;

    public OrgEvent(long id, String title, long date) {
        super(id);
        this.title = title;
        this.date = date;
        this.attendees = new ArrayList<>();
    }

    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public long getDate() { return date; }
    public Organization getOrganization() { return organization; }
    public void setOrganization(Organization org) { this.organization = org; }
    public List<Object> getAttendees() { return attendees; }
    public void addAttendee(Object e) { attendees.add(e); }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
}
