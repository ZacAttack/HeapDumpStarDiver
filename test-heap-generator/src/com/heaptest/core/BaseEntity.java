// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.core;

public abstract class BaseEntity implements Timestamped {
    protected long id;
    protected long createdAt;
    protected long updatedAt;

    public BaseEntity(long id) {
        this.id = id;
        this.createdAt = System.currentTimeMillis();
        this.updatedAt = this.createdAt;
    }

    @Override
    public long getId() { return id; }
    @Override
    public long getCreatedAt() { return createdAt; }
    @Override
    public long getUpdatedAt() { return updatedAt; }

    public void touch() { this.updatedAt = System.currentTimeMillis(); }
}
