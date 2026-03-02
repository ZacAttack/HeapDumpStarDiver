package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.CoreEnums;

public class Deployment extends AuditableEntity {
    private Server server;
    private Object project; // project.Project
    private String deployVersion;
    private long timestamp;
    private CoreEnums.Status status;
    private String deployedBy;
    private String environment; // PROD, STAGING, DEV

    public Deployment(long id, Server server, String deployVersion) {
        super(id, "system");
        this.server = server;
        this.deployVersion = deployVersion;
        this.timestamp = System.currentTimeMillis();
        this.status = CoreEnums.Status.ACTIVE;
    }

    public Server getServer() { return server; }
    public Object getProject() { return project; }
    public void setProject(Object p) { this.project = p; }
    public String getDeployVersion() { return deployVersion; }
    public long getTimestamp() { return timestamp; }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public String getDeployedBy() { return deployedBy; }
    public void setDeployedBy(String by) { this.deployedBy = by; }
    public String getEnvironment() { return environment; }
    public void setEnvironment(String env) { this.environment = env; }
}
