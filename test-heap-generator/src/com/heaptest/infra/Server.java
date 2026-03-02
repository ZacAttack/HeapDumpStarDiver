package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Server extends AuditableEntity {
    private String hostname;
    private String ipAddress;
    private int cpuCores;
    private double memoryGb;
    private String os;
    private List<Deployment> deployments;
    private boolean active;
    private double cpuUtilization;

    public Server(long id, String hostname, String ipAddress, int cpuCores, double memoryGb) {
        super(id, "system");
        this.hostname = hostname;
        this.ipAddress = ipAddress;
        this.cpuCores = cpuCores;
        this.memoryGb = memoryGb;
        this.deployments = new ArrayList<>();
        this.active = true;
    }

    public String getHostname() { return hostname; }
    public String getIpAddress() { return ipAddress; }
    public int getCpuCores() { return cpuCores; }
    public double getMemoryGb() { return memoryGb; }
    public String getOs() { return os; }
    public void setOs(String os) { this.os = os; }
    public List<Deployment> getDeployments() { return deployments; }
    public void addDeployment(Deployment d) { deployments.add(d); }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public double getCpuUtilization() { return cpuUtilization; }
    public void setCpuUtilization(double u) { this.cpuUtilization = u; }
}
