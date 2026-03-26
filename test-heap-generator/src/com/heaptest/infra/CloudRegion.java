// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class CloudRegion extends AuditableEntity {
    private String name;
    private String code;
    private List<Server> servers;
    private double availability;
    private String provider; // AWS, GCP, AZURE
    private int availabilityZones;

    public CloudRegion(long id, String name, String code) {
        super(id, "system");
        this.name = name;
        this.code = code;
        this.servers = new ArrayList<>();
        this.availability = 99.99;
    }

    public String getName() { return name; }
    public String getCode() { return code; }
    public List<Server> getServers() { return servers; }
    public void addServer(Server s) { servers.add(s); }
    public double getAvailability() { return availability; }
    public void setAvailability(double a) { this.availability = a; }
    public String getProvider() { return provider; }
    public void setProvider(String p) { this.provider = p; }
    public int getAvailabilityZones() { return availabilityZones; }
    public void setAvailabilityZones(int az) { this.availabilityZones = az; }
}
