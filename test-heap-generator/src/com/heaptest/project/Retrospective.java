// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.project;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Retrospective extends AuditableEntity {
    private Sprint sprint;
    private List<String> goodItems;
    private List<String> badItems;
    private List<String> actionItems;
    private int participantCount;

    public Retrospective(long id, Sprint sprint) {
        super(id, "system");
        this.sprint = sprint;
        this.goodItems = new ArrayList<>();
        this.badItems = new ArrayList<>();
        this.actionItems = new ArrayList<>();
    }

    public Sprint getSprint() { return sprint; }
    public List<String> getGoodItems() { return goodItems; }
    public void addGoodItem(String item) { goodItems.add(item); }
    public List<String> getBadItems() { return badItems; }
    public void addBadItem(String item) { badItems.add(item); }
    public List<String> getActionItems() { return actionItems; }
    public void addActionItem(String item) { actionItems.add(item); }
    public int getParticipantCount() { return participantCount; }
    public void setParticipantCount(int count) { this.participantCount = count; }
}
