package com.heaptest.analytics;

import com.heaptest.core.BaseEntity;
import java.util.HashMap;
import java.util.Map;

public class DataPoint extends BaseEntity {
    private String metricName;
    private double value;
    private long timestamp;
    private Map<String, String> dimensions;
    private String unit;

    public DataPoint(long id, String metricName, double value, long timestamp) {
        super(id);
        this.metricName = metricName;
        this.value = value;
        this.timestamp = timestamp;
        this.dimensions = new HashMap<>();
    }

    public String getMetricName() { return metricName; }
    public double getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    public Map<String, String> getDimensions() { return dimensions; }
    public void addDimension(String key, String value) { dimensions.put(key, value); }
    public String getUnit() { return unit; }
    public void setUnit(String unit) { this.unit = unit; }
}
