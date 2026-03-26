// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.core;

import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator {
    private AtomicLong counter;
    private long[] recentIds;
    private int[] checksums;
    private int recentIndex;
    private static final int RECENT_SIZE = 1024;

    public IdGenerator(long startFrom) {
        this.counter = new AtomicLong(startFrom);
        this.recentIds = new long[RECENT_SIZE];
        this.checksums = new int[RECENT_SIZE];
        this.recentIndex = 0;
    }

    public long nextId() {
        long id = counter.incrementAndGet();
        int idx = recentIndex % RECENT_SIZE;
        recentIds[idx] = id;
        checksums[idx] = Long.hashCode(id);
        recentIndex++;
        return id;
    }

    public long current() { return counter.get(); }
    public long[] getRecentIds() { return recentIds; }
    public int[] getChecksums() { return checksums; }
}
