package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Database extends AuditableEntity {
    private String name;
    private Server server;
    private String engine; // POSTGRESQL, MYSQL, MONGODB, REDIS
    private double sizeGb;
    private List<Database> replicas;
    private int maxConnections;
    private int activeConnections;

    public Database(long id, String name, Server server, String engine) {
        super(id, "system");
        this.name = name;
        this.server = server;
        this.engine = engine;
        this.sizeGb = 0.0;
        this.replicas = new ArrayList<>();
        this.maxConnections = 100;
        this.activeConnections = 0;
    }

    public String getName() { return name; }
    public Server getServer() { return server; }
    public String getEngine() { return engine; }
    public double getSizeGb() { return sizeGb; }
    public void setSizeGb(double size) { this.sizeGb = size; }
    public List<Database> getReplicas() { return replicas; }
    public void addReplica(Database r) { replicas.add(r); }
    public int getMaxConnections() { return maxConnections; }
    public void setMaxConnections(int max) { this.maxConnections = max; }
    public int getActiveConnections() { return activeConnections; }
    public void setActiveConnections(int active) { this.activeConnections = active; }
}
