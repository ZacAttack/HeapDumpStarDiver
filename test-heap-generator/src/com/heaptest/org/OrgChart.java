// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.org;

import com.heaptest.core.BaseEntity;
import java.util.ArrayList;
import java.util.List;

public class OrgChart extends BaseEntity {
    private Organization organization;
    private List<Division> rootDivisions;
    private List<String> flattenedNodes;

    public OrgChart(long id, Organization org) {
        super(id);
        this.organization = org;
        this.rootDivisions = new ArrayList<>();
        this.flattenedNodes = new ArrayList<>();
    }

    public Organization getOrganization() { return organization; }
    public List<Division> getRootDivisions() { return rootDivisions; }
    public List<String> getFlattenedNodes() { return flattenedNodes; }
    public void addDivision(Division d) { rootDivisions.add(d); }
    public void addNode(String node) { flattenedNodes.add(node); }
}
