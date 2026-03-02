package com.heaptest.org;

import com.heaptest.core.BaseEntity;
import java.util.HashMap;
import java.util.Map;

public class OrgMetrics extends BaseEntity {
    private Organization organization;
    private int headcount;
    private double revenue;
    private double profitMargin;
    private Map<String, Double> metrics;

    public OrgMetrics(long id, Organization org) {
        super(id);
        this.organization = org;
        this.metrics = new HashMap<>();
    }

    public Organization getOrganization() { return organization; }
    public int getHeadcount() { return headcount; }
    public void setHeadcount(int headcount) { this.headcount = headcount; }
    public double getRevenue() { return revenue; }
    public void setRevenue(double revenue) { this.revenue = revenue; }
    public double getProfitMargin() { return profitMargin; }
    public void setProfitMargin(double margin) { this.profitMargin = margin; }
    public Map<String, Double> getMetrics() { return metrics; }
    public void setMetric(String key, double value) { metrics.put(key, value); }
}
