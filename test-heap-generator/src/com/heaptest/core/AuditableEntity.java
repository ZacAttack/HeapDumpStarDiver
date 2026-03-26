// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.core;

public abstract class AuditableEntity extends BaseEntity {
    protected String createdBy;
    protected String updatedBy;
    protected int version;

    public AuditableEntity(long id, String createdBy) {
        super(id);
        this.createdBy = createdBy;
        this.updatedBy = createdBy;
        this.version = 1;
    }

    public String getCreatedBy() { return createdBy; }
    public String getUpdatedBy() { return updatedBy; }
    public int getVersion() { return version; }

    public void update(String updatedBy) {
        this.updatedBy = updatedBy;
        this.version++;
        touch();
    }
}
