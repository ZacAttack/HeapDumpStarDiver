// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.security;

import com.heaptest.core.BaseEntity;

public class Permission extends BaseEntity {
    private String resource;
    private String action; // READ, WRITE, DELETE, ADMIN
    private boolean granted;
    private String scope; // GLOBAL, ORG, TEAM, SELF

    public Permission(long id, String resource, String action, boolean granted) {
        super(id);
        this.resource = resource;
        this.action = action;
        this.granted = granted;
    }

    public String getResource() { return resource; }
    public String getAction() { return action; }
    public boolean isGranted() { return granted; }
    public void setGranted(boolean g) { this.granted = g; }
    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }
}
