// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Monitor extends AuditableEntity {
    private Server target;
    private String metricName;
    private float[] recentValues;
    private double threshold;
    private boolean alerting;
    private List<Alert> alerts;
    private int checkIntervalSeconds;

    public Monitor(long id, Server target, String metricName, int historySize) {
        super(id, "system");
        this.target = target;
        this.metricName = metricName;
        this.recentValues = new float[historySize];
        this.threshold = 0.0;
        this.alerting = false;
        this.alerts = new ArrayList<>();
        this.checkIntervalSeconds = 60;
    }

    public Server getTarget() { return target; }
    public String getMetricName() { return metricName; }
    public float[] getRecentValues() { return recentValues; }
    public void recordValue(int idx, float value) { recentValues[idx % recentValues.length] = value; }
    public double getThreshold() { return threshold; }
    public void setThreshold(double t) { this.threshold = t; }
    public boolean isAlerting() { return alerting; }
    public void setAlerting(boolean a) { this.alerting = a; }
    public List<Alert> getAlerts() { return alerts; }
    public void addAlert(Alert a) { alerts.add(a); }
    public int getCheckIntervalSeconds() { return checkIntervalSeconds; }
    public void setCheckIntervalSeconds(int s) { this.checkIntervalSeconds = s; }
}
