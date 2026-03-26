// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Firewall extends AuditableEntity {
    private String name;
    private List<String> rules;
    private Network network;
    private boolean enabled;
    private String defaultPolicy; // ALLOW, DENY
    private int ruleCount;

    public Firewall(long id, String name, Network network) {
        super(id, "system");
        this.name = name;
        this.network = network;
        this.rules = new ArrayList<>();
        this.enabled = true;
        this.defaultPolicy = "DENY";
    }

    public String getName() { return name; }
    public List<String> getRules() { return rules; }
    public void addRule(String rule) { rules.add(rule); ruleCount++; }
    public Network getNetwork() { return network; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean e) { this.enabled = e; }
    public String getDefaultPolicy() { return defaultPolicy; }
    public void setDefaultPolicy(String policy) { this.defaultPolicy = policy; }
    public int getRuleCount() { return ruleCount; }
}
