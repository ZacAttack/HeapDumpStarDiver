// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.comms;

import com.heaptest.core.BaseEntity;
import java.util.ArrayList;
import java.util.List;

public class Message extends BaseEntity {
    private Object sender; // hr.Employee
    private Channel channel;
    private String content;
    private Message parentMessage; // self-reference!
    private List<Attachment> attachments;
    private long timestamp;
    private boolean edited;
    private boolean deleted;

    public Message(long id, Object sender, Channel channel, String content) {
        super(id);
        this.sender = sender;
        this.channel = channel;
        this.content = content;
        this.attachments = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
        this.edited = false;
        this.deleted = false;
    }

    public Object getSender() { return sender; }
    public Channel getChannel() { return channel; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; this.edited = true; }
    public Message getParentMessage() { return parentMessage; }
    public void setParentMessage(Message parent) { this.parentMessage = parent; }
    public List<Attachment> getAttachments() { return attachments; }
    public void addAttachment(Attachment a) { attachments.add(a); }
    public long getTimestamp() { return timestamp; }
    public boolean isEdited() { return edited; }
    public boolean isDeleted() { return deleted; }
    public void setDeleted(boolean d) { this.deleted = d; }
}
