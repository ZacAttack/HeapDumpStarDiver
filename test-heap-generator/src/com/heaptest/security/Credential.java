// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.security;

import com.heaptest.core.AuditableEntity;

public class Credential extends AuditableEntity {
    private UserAccount userAccount;
    private String type; // PASSWORD, API_KEY, SSH_KEY, OAUTH
    private String value;
    private long expiresAt;
    private boolean revoked;

    public Credential(long id, UserAccount user, String type) {
        super(id, "system");
        this.userAccount = user;
        this.type = type;
        this.expiresAt = System.currentTimeMillis() + 90L * 24 * 60 * 60 * 1000;
        this.revoked = false;
    }

    public UserAccount getUserAccount() { return userAccount; }
    public String getType() { return type; }
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
    public long getExpiresAt() { return expiresAt; }
    public void setExpiresAt(long ts) { this.expiresAt = ts; }
    public boolean isRevoked() { return revoked; }
    public void setRevoked(boolean r) { this.revoked = r; }
}
