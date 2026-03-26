// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Release extends AuditableEntity {
    private String releaseVersion;
    private Project project;
    private List<Milestone> milestones;
    private short[] buildMetrics;
    private long releaseDate;
    private String releaseNotes;
    private boolean deployed;

    public Release(long id, String releaseVersion, Project project) {
        super(id, "system");
        this.releaseVersion = releaseVersion;
        this.project = project;
        this.milestones = new ArrayList<>();
        this.buildMetrics = new short[20]; // 20 build metrics
        this.deployed = false;
    }

    public String getReleaseVersion() { return releaseVersion; }
    public Project getProject() { return project; }
    public List<Milestone> getMilestones() { return milestones; }
    public void addMilestone(Milestone m) { milestones.add(m); }
    public short[] getBuildMetrics() { return buildMetrics; }
    public void setBuildMetric(int idx, short value) { buildMetrics[idx] = value; }
    public long getReleaseDate() { return releaseDate; }
    public void setReleaseDate(long date) { this.releaseDate = date; }
    public String getReleaseNotes() { return releaseNotes; }
    public void setReleaseNotes(String notes) { this.releaseNotes = notes; }
    public boolean isDeployed() { return deployed; }
    public void setDeployed(boolean d) { this.deployed = d; }
}
