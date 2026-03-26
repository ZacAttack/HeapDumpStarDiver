// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.TaggableEntity;
import com.heaptest.core.CoreEnums;
import java.util.ArrayList;
import java.util.List;

public class Project extends TaggableEntity {
    private String name;
    private String code;
    private Object team; // org.Team
    private List<Sprint> sprints;
    private Backlog backlog;
    private List<Milestone> milestones;
    private CoreEnums.Status status;
    private long startDate;
    private Long endDate; // nullable

    public Project(long id, String name, String code) {
        super(id, "system");
        this.name = name;
        this.code = code;
        this.sprints = new ArrayList<>();
        this.milestones = new ArrayList<>();
        this.status = CoreEnums.Status.ACTIVE;
        this.startDate = System.currentTimeMillis();
    }

    public String getName() { return name; }
    public String getCode() { return code; }
    public Object getTeam() { return team; }
    public void setTeam(Object team) { this.team = team; }
    public List<Sprint> getSprints() { return sprints; }
    public void addSprint(Sprint s) { sprints.add(s); }
    public Backlog getBacklog() { return backlog; }
    public void setBacklog(Backlog b) { this.backlog = b; }
    public List<Milestone> getMilestones() { return milestones; }
    public void addMilestone(Milestone m) { milestones.add(m); }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public long getStartDate() { return startDate; }
    public Long getEndDate() { return endDate; }
    public void setEndDate(Long date) { this.endDate = date; }
}
