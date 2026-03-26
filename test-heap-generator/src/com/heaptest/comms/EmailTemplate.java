// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.comms;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class EmailTemplate extends AuditableEntity {
    private String name;
    private String subject;
    private String body;
    private List<String> variables;
    private boolean active;

    public EmailTemplate(long id, String name, String subject, String body) {
        super(id, "system");
        this.name = name;
        this.subject = subject;
        this.body = body;
        this.variables = new ArrayList<>();
        this.active = true;
    }

    public String getName() { return name; }
    public String getSubject() { return subject; }
    public String getBody() { return body; }
    public List<String> getVariables() { return variables; }
    public void addVariable(String var) { variables.add(var); }
    public boolean isActive() { return active; }
    public void setActive(boolean a) { this.active = a; }
}
