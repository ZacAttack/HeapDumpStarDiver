// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;

public class RiskItem extends AuditableEntity {
    private String description;
    private float probability;
    private float impact;
    private String mitigation;
    private String status; // OPEN, MITIGATED, CLOSED, ACCEPTED
    private String category;

    public RiskItem(long id, String description, float probability, float impact) {
        super(id, "system");
        this.description = description;
        this.probability = probability;
        this.impact = impact;
        this.status = "OPEN";
    }

    public String getDescription() { return description; }
    public float getProbability() { return probability; }
    public float getImpact() { return impact; }
    public float getRiskScore() { return probability * impact; }
    public String getMitigation() { return mitigation; }
    public void setMitigation(String m) { this.mitigation = m; }
    public String getStatus() { return status; }
    public void setStatus(String s) { this.status = s; }
    public String getCategory() { return category; }
    public void setCategory(String c) { this.category = c; }
}
