package com.heaptest.security;

import com.heaptest.core.BaseEntity;

public class AuditLog extends BaseEntity {
    private UserAccount userAccount;
    private String action;
    private String resource;
    private long timestamp;
    private String ipAddress;
    private boolean success;
    private String details;

    public AuditLog(long id, UserAccount user, String action, String resource) {
        super(id);
        this.userAccount = user;
        this.action = action;
        this.resource = resource;
        this.timestamp = System.currentTimeMillis();
        this.success = true;
    }

    public UserAccount getUserAccount() { return userAccount; }
    public String getAction() { return action; }
    public String getResource() { return resource; }
    public long getTimestamp() { return timestamp; }
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ip) { this.ipAddress = ip; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean s) { this.success = s; }
    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }
}
