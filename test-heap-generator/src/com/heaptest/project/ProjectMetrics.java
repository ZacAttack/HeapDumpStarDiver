// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.BaseEntity;

public class ProjectMetrics extends BaseEntity {
    private Project project;
    private double[] burndownData;
    private int[] velocityHistory;
    private int totalTasks;
    private int completedTasks;
    private double completionPercentage;

    public ProjectMetrics(long id, Project project, int sprintCount) {
        super(id);
        this.project = project;
        this.burndownData = new double[sprintCount * 10]; // daily data points
        this.velocityHistory = new int[sprintCount];
        this.totalTasks = 0;
        this.completedTasks = 0;
    }

    public Project getProject() { return project; }
    public double[] getBurndownData() { return burndownData; }
    public void setBurndownPoint(int idx, double value) { burndownData[idx] = value; }
    public int[] getVelocityHistory() { return velocityHistory; }
    public void setVelocity(int sprint, int velocity) { velocityHistory[sprint] = velocity; }
    public int getTotalTasks() { return totalTasks; }
    public void setTotalTasks(int t) { this.totalTasks = t; }
    public int getCompletedTasks() { return completedTasks; }
    public void setCompletedTasks(int c) { this.completedTasks = c; }
    public double getCompletionPercentage() { return completionPercentage; }
    public void setCompletionPercentage(double p) { this.completionPercentage = p; }
}
