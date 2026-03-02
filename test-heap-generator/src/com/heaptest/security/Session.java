package com.heaptest.security;

import com.heaptest.core.BaseEntity;

public class Session extends BaseEntity {
    private UserAccount userAccount;
    private String token;
    private long startedAt;
    private long lastActivity;
    private boolean active;
    private String ipAddress;
    private String userAgent;

    public Session(long id, UserAccount user, String token) {
        super(id);
        this.userAccount = user;
        this.token = token;
        this.startedAt = System.currentTimeMillis();
        this.lastActivity = this.startedAt;
        this.active = true;
    }

    public UserAccount getUserAccount() { return userAccount; }
    public String getToken() { return token; }
    public long getStartedAt() { return startedAt; }
    public long getLastActivity() { return lastActivity; }
    public void updateActivity() { this.lastActivity = System.currentTimeMillis(); }
    public boolean isActive() { return active; }
    public void setActive(boolean a) { this.active = a; }
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ip) { this.ipAddress = ip; }
    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String ua) { this.userAgent = ua; }
}
