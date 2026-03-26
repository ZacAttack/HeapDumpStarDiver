// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.analytics;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Dashboard extends AuditableEntity {
    private String name;
    private List<Widget> widgets;
    private Object owner; // hr.Employee
    private boolean shared;
    private String layout; // GRID, FREEFORM

    public Dashboard(long id, String name) {
        super(id, "system");
        this.name = name;
        this.widgets = new ArrayList<>();
        this.shared = false;
        this.layout = "GRID";
    }

    public String getName() { return name; }
    public List<Widget> getWidgets() { return widgets; }
    public void addWidget(Widget w) { widgets.add(w); }
    public Object getOwner() { return owner; }
    public void setOwner(Object owner) { this.owner = owner; }
    public boolean isShared() { return shared; }
    public void setShared(boolean shared) { this.shared = shared; }
    public String getLayout() { return layout; }
    public void setLayout(String layout) { this.layout = layout; }
}
