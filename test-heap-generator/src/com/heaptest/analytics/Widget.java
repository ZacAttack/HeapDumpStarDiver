// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.analytics;

import com.heaptest.core.AuditableEntity;
import java.util.TreeMap;

public class Widget extends AuditableEntity {
    private String title;
    private String type; // CHART, TABLE, GAUGE, MAP
    private Dashboard dashboard;
    private String dataSource;
    private TreeMap<String, String> config;
    private int width;
    private int height;

    public Widget(long id, String title, String type, Dashboard dashboard) {
        super(id, "system");
        this.title = title;
        this.type = type;
        this.dashboard = dashboard;
        this.config = new TreeMap<>();
        this.width = 1;
        this.height = 1;
    }

    public String getTitle() { return title; }
    public String getType() { return type; }
    public Dashboard getDashboard() { return dashboard; }
    public String getDataSource() { return dataSource; }
    public void setDataSource(String ds) { this.dataSource = ds; }
    public TreeMap<String, String> getConfig() { return config; }
    public void setConfig(String key, String value) { config.put(key, value); }
    public int getWidth() { return width; }
    public void setWidth(int w) { this.width = w; }
    public int getHeight() { return height; }
    public void setHeight(int h) { this.height = h; }
}
