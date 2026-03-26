// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.analytics;

import com.heaptest.core.BaseEntity;

public class Trend extends BaseEntity {
    private MetricDefinition metric;
    private long[] timestamps;
    private double[] values;
    private String direction; // UP, DOWN, STABLE
    private double changePercent;

    public Trend(long id, MetricDefinition metric, int dataPoints) {
        super(id);
        this.metric = metric;
        this.timestamps = new long[dataPoints];
        this.values = new double[dataPoints];
        this.direction = "STABLE";
    }

    public MetricDefinition getMetric() { return metric; }
    public long[] getTimestamps() { return timestamps; }
    public void setTimestamp(int idx, long ts) { timestamps[idx] = ts; }
    public double[] getValues() { return values; }
    public void setValue(int idx, double val) { values[idx] = val; }
    public String getDirection() { return direction; }
    public void setDirection(String dir) { this.direction = dir; }
    public double getChangePercent() { return changePercent; }
    public void setChangePercent(double pct) { this.changePercent = pct; }
}
