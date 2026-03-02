package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class LoadBalancer extends AuditableEntity {
    private String name;
    private List<Server> servers;
    private String algorithm; // ROUND_ROBIN, LEAST_CONNECTIONS, WEIGHTED
    private int[] connectionDistribution;
    private boolean healthCheckEnabled;
    private int port;

    public LoadBalancer(long id, String name, String algorithm) {
        super(id, "system");
        this.name = name;
        this.algorithm = algorithm;
        this.servers = new ArrayList<>();
        this.connectionDistribution = new int[0];
        this.healthCheckEnabled = true;
        this.port = 443;
    }

    public String getName() { return name; }
    public List<Server> getServers() { return servers; }
    public void addServer(Server s) {
        servers.add(s);
        connectionDistribution = new int[servers.size()];
    }
    public String getAlgorithm() { return algorithm; }
    public int[] getConnectionDistribution() { return connectionDistribution; }
    public void setConnectionCount(int serverIdx, int count) {
        if (serverIdx < connectionDistribution.length) connectionDistribution[serverIdx] = count;
    }
    public boolean isHealthCheckEnabled() { return healthCheckEnabled; }
    public void setHealthCheckEnabled(boolean h) { this.healthCheckEnabled = h; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
}
