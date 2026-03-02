package com.heaptest.analytics;

import com.heaptest.core.BaseEntity;
import com.heaptest.core.CoreEnums;

public class Anomaly extends BaseEntity {
    private MetricDefinition metric;
    private long detectedAt;
    private double value;
    private double expectedValue;
    private CoreEnums.Severity severity;
    private boolean resolved;
    private String notes;

    public Anomaly(long id, MetricDefinition metric, double value, double expectedValue) {
        super(id);
        this.metric = metric;
        this.value = value;
        this.expectedValue = expectedValue;
        this.detectedAt = System.currentTimeMillis();
        this.severity = CoreEnums.Severity.INFO;
        this.resolved = false;
    }

    public MetricDefinition getMetric() { return metric; }
    public long getDetectedAt() { return detectedAt; }
    public double getValue() { return value; }
    public double getExpectedValue() { return expectedValue; }
    public CoreEnums.Severity getSeverity() { return severity; }
    public void setSeverity(CoreEnums.Severity s) { this.severity = s; }
    public boolean isResolved() { return resolved; }
    public void setResolved(boolean r) { this.resolved = r; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
}
