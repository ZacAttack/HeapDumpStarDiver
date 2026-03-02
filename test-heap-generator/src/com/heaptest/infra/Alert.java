package com.heaptest.infra;

import com.heaptest.core.BaseEntity;
import com.heaptest.core.CoreEnums;

public class Alert extends BaseEntity {
    private Monitor monitor;
    private CoreEnums.Severity severity;
    private String message;
    private boolean acknowledged;
    private long timestamp;
    private String acknowledgedBy;

    public Alert(long id, Monitor monitor, CoreEnums.Severity severity, String message) {
        super(id);
        this.monitor = monitor;
        this.severity = severity;
        this.message = message;
        this.acknowledged = false;
        this.timestamp = System.currentTimeMillis();
    }

    public Monitor getMonitor() { return monitor; }
    public CoreEnums.Severity getSeverity() { return severity; }
    public String getMessage() { return message; }
    public boolean isAcknowledged() { return acknowledged; }
    public void setAcknowledged(boolean ack) { this.acknowledged = ack; }
    public long getTimestamp() { return timestamp; }
    public String getAcknowledgedBy() { return acknowledgedBy; }
    public void setAcknowledgedBy(String by) { this.acknowledgedBy = by; }
}
