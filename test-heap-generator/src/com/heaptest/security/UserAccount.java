package com.heaptest.security;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class UserAccount extends AuditableEntity {
    private String username;
    private byte[] passwordHash;
    private byte[] salt;
    private char[] totpSecret;
    private Object employee; // hr.Employee
    private List<Permission> roles;
    private List<Session> sessions;
    private boolean locked;
    private int failedAttempts;
    private long lastLogin;

    public UserAccount(long id, String username) {
        super(id, "system");
        this.username = username;
        this.roles = new ArrayList<>();
        this.sessions = new ArrayList<>();
        this.locked = false;
        this.failedAttempts = 0;
    }

    public String getUsername() { return username; }
    public byte[] getPasswordHash() { return passwordHash; }
    public void setPasswordHash(byte[] hash) { this.passwordHash = hash; }
    public byte[] getSalt() { return salt; }
    public void setSalt(byte[] salt) { this.salt = salt; }
    public char[] getTotpSecret() { return totpSecret; }
    public void setTotpSecret(char[] secret) { this.totpSecret = secret; }
    public Object getEmployee() { return employee; }
    public void setEmployee(Object emp) { this.employee = emp; }
    public List<Permission> getRoles() { return roles; }
    public void addRole(Permission p) { roles.add(p); }
    public List<Session> getSessions() { return sessions; }
    public void addSession(Session s) { sessions.add(s); }
    public boolean isLocked() { return locked; }
    public void setLocked(boolean locked) { this.locked = locked; }
    public int getFailedAttempts() { return failedAttempts; }
    public void incrementFailedAttempts() { this.failedAttempts++; if (failedAttempts >= 5) locked = true; }
    public long getLastLogin() { return lastLogin; }
    public void setLastLogin(long ts) { this.lastLogin = ts; }
}
