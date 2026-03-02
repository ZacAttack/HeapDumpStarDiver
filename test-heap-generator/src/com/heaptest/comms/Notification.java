package com.heaptest.comms;

import com.heaptest.core.BaseEntity;

public class Notification extends BaseEntity {
    private Object recipient; // hr.Employee
    private String title;
    private String body;
    private boolean read;
    private Channel channel;
    private String type; // INFO, WARNING, ACTION_REQUIRED
    private long sentAt;

    public Notification(long id, Object recipient, String title, String body) {
        super(id);
        this.recipient = recipient;
        this.title = title;
        this.body = body;
        this.read = false;
        this.sentAt = System.currentTimeMillis();
    }

    public Object getRecipient() { return recipient; }
    public String getTitle() { return title; }
    public String getBody() { return body; }
    public boolean isRead() { return read; }
    public void setRead(boolean r) { this.read = r; }
    public Channel getChannel() { return channel; }
    public void setChannel(Channel c) { this.channel = c; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public long getSentAt() { return sentAt; }
}
