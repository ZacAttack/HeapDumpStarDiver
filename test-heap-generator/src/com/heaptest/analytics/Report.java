package com.heaptest.analytics;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.DateRange;
import java.util.ArrayList;
import java.util.List;

public class Report extends AuditableEntity {
    private String title;
    private List<DataPoint> dataPoints;
    private Object author; // hr.Employee
    private DateRange dateRange;
    private String format; // PDF, CSV, HTML
    private boolean published;

    public Report(long id, String title, DateRange dateRange) {
        super(id, "system");
        this.title = title;
        this.dateRange = dateRange;
        this.dataPoints = new ArrayList<>();
        this.published = false;
    }

    public String getTitle() { return title; }
    public List<DataPoint> getDataPoints() { return dataPoints; }
    public void addDataPoint(DataPoint dp) { dataPoints.add(dp); }
    public Object getAuthor() { return author; }
    public void setAuthor(Object author) { this.author = author; }
    public DateRange getDateRange() { return dateRange; }
    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }
    public boolean isPublished() { return published; }
    public void setPublished(boolean p) { this.published = p; }
}
