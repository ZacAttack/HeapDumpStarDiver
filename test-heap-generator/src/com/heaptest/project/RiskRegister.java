// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class RiskRegister extends AuditableEntity {
    private Project project;
    private List<RiskItem> items;
    private int openRisks;
    private int mitigatedRisks;

    public RiskRegister(long id, Project project) {
        super(id, "system");
        this.project = project;
        this.items = new ArrayList<>();
        this.openRisks = 0;
        this.mitigatedRisks = 0;
    }

    public Project getProject() { return project; }
    public List<RiskItem> getItems() { return items; }
    public void addItem(RiskItem item) { items.add(item); openRisks++; }
    public int getOpenRisks() { return openRisks; }
    public int getMitigatedRisks() { return mitigatedRisks; }
    public void mitigateRisk() { openRisks--; mitigatedRisks++; }
}
