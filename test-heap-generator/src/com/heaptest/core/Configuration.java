package com.heaptest.core;

import java.util.HashMap;
import java.util.Map;

public class Configuration extends BaseEntity {
    private String name;
    private Map<String, String> properties;
    private boolean[] featureToggles;

    public Configuration(long id, String name, int toggleCount) {
        super(id);
        this.name = name;
        this.properties = new HashMap<>();
        this.featureToggles = new boolean[toggleCount];
    }

    public void setProperty(String key, String value) { properties.put(key, value); }
    public String getProperty(String key) { return properties.get(key); }
    public void setToggle(int index, boolean value) { featureToggles[index] = value; }
    public boolean getToggle(int index) { return featureToggles[index]; }
    public String getName() { return name; }
    public Map<String, String> getProperties() { return properties; }
    public boolean[] getFeatureToggles() { return featureToggles; }
}
