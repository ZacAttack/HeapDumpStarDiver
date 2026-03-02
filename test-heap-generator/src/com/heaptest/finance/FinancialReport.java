package com.heaptest.finance;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.DateRange;
import java.util.HashMap;
import java.util.Map;

public class FinancialReport extends AuditableEntity {
    private String title;
    private DateRange period;
    private Map<String, Double> data;
    private String type; // QUARTERLY, ANNUAL, MONTHLY
    private boolean published;

    public FinancialReport(long id, String title, DateRange period) {
        super(id, "system");
        this.title = title;
        this.period = period;
        this.data = new HashMap<>();
        this.published = false;
    }

    public String getTitle() { return title; }
    public DateRange getPeriod() { return period; }
    public Map<String, Double> getData() { return data; }
    public void addData(String key, double value) { data.put(key, value); }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public boolean isPublished() { return published; }
    public void setPublished(boolean pub) { this.published = pub; }
}
