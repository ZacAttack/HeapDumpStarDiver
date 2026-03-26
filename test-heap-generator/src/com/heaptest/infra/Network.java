// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Network extends AuditableEntity {
    private String name;
    private String cidr;
    private int vlanId;
    private List<Server> servers;
    private boolean encrypted;
    private String gateway;

    public Network(long id, String name, String cidr, int vlanId) {
        super(id, "system");
        this.name = name;
        this.cidr = cidr;
        this.vlanId = vlanId;
        this.servers = new ArrayList<>();
        this.encrypted = false;
    }

    public String getName() { return name; }
    public String getCidr() { return cidr; }
    public int getVlanId() { return vlanId; }
    public List<Server> getServers() { return servers; }
    public void addServer(Server s) { servers.add(s); }
    public boolean isEncrypted() { return encrypted; }
    public void setEncrypted(boolean e) { this.encrypted = e; }
    public String getGateway() { return gateway; }
    public void setGateway(String gw) { this.gateway = gw; }
}
