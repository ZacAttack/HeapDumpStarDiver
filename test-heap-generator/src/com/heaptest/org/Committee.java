// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Committee extends AuditableEntity {
    private String name;
    private Object chairperson; // Employee ref
    private List<Object> members; // Employee refs
    private List<Long> meetingDates;
    private String charter;

    public Committee(long id, String name) {
        super(id, "system");
        this.name = name;
        this.members = new ArrayList<>();
        this.meetingDates = new ArrayList<>();
    }

    public String getName() { return name; }
    public Object getChairperson() { return chairperson; }
    public void setChairperson(Object chair) { this.chairperson = chair; }
    public List<Object> getMembers() { return members; }
    public void addMember(Object e) { members.add(e); }
    public List<Long> getMeetingDates() { return meetingDates; }
    public void addMeetingDate(long date) { meetingDates.add(date); }
    public String getCharter() { return charter; }
    public void setCharter(String charter) { this.charter = charter; }
}
