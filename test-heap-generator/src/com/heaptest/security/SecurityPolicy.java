// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.security;

import com.heaptest.core.AuditableEntity;
import java.util.concurrent.ConcurrentHashMap;

public class SecurityPolicy extends AuditableEntity {
    private String name;
    private ConcurrentHashMap<String, String> rules;
    private boolean enforced;
    private String description;
    private int maxSessionDurationMinutes;

    public SecurityPolicy(long id, String name) {
        super(id, "system");
        this.name = name;
        this.rules = new ConcurrentHashMap<>();
        this.enforced = true;
        this.maxSessionDurationMinutes = 480;
    }

    public String getName() { return name; }
    public ConcurrentHashMap<String, String> getRules() { return rules; }
    public void addRule(String key, String value) { rules.put(key, value); }
    public boolean isEnforced() { return enforced; }
    public void setEnforced(boolean e) { this.enforced = e; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public int getMaxSessionDurationMinutes() { return maxSessionDurationMinutes; }
    public void setMaxSessionDurationMinutes(int mins) { this.maxSessionDurationMinutes = mins; }
}
