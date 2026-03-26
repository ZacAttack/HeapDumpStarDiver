// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.AuditableEntity;

public class OrgPolicy extends AuditableEntity {
    private String title;
    private String description;
    private long effectiveDate;
    private long expirationDate;
    private boolean mandatory;
    private Organization organization;

    public OrgPolicy(long id, String title, String description) {
        super(id, "system");
        this.title = title;
        this.description = description;
        this.effectiveDate = System.currentTimeMillis();
        this.expirationDate = effectiveDate + 365L * 24 * 60 * 60 * 1000;
        this.mandatory = true;
    }

    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public long getEffectiveDate() { return effectiveDate; }
    public long getExpirationDate() { return expirationDate; }
    public boolean isMandatory() { return mandatory; }
    public void setMandatory(boolean mandatory) { this.mandatory = mandatory; }
    public Organization getOrganization() { return organization; }
    public void setOrganization(Organization org) { this.organization = org; }
}
