package com.heaptest.core;

public interface Timestamped extends Identifiable {
    long getCreatedAt();
    long getUpdatedAt();
}
