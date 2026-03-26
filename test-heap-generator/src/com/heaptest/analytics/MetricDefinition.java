// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.analytics;

import com.heaptest.core.AuditableEntity;

public class MetricDefinition extends AuditableEntity {
    private String name;
    private String unit;
    private String aggregation; // SUM, AVG, MIN, MAX, COUNT
    private String description;
    private double minValue;
    private double maxValue;

    public MetricDefinition(long id, String name, String unit, String aggregation) {
        super(id, "system");
        this.name = name;
        this.unit = unit;
        this.aggregation = aggregation;
        this.minValue = Double.MIN_VALUE;
        this.maxValue = Double.MAX_VALUE;
    }

    public String getName() { return name; }
    public String getUnit() { return unit; }
    public String getAggregation() { return aggregation; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public double getMinValue() { return minValue; }
    public void setMinValue(double min) { this.minValue = min; }
    public double getMaxValue() { return maxValue; }
    public void setMaxValue(double max) { this.maxValue = max; }
}
