package com.heaptest.core;

public class DateRange {
    private long startDate;
    private long endDate;
    private String label;

    public DateRange(long startDate, long endDate, String label) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.label = label;
    }

    public long getStartDate() { return startDate; }
    public long getEndDate() { return endDate; }
    public String getLabel() { return label; }
}
