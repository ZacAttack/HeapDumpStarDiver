package com.heaptest.core;

public class CoreEnums {
    public enum Status {
        ACTIVE, INACTIVE, PENDING, ARCHIVED, DELETED, SUSPENDED
    }

    public enum Priority {
        CRITICAL, HIGH, MEDIUM, LOW, NONE
    }

    public enum Severity {
        BLOCKER, CRITICAL, MAJOR, MINOR, TRIVIAL, INFO
    }
}
