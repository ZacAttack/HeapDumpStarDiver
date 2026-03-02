package com.heaptest.comms;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;

public class Channel extends AuditableEntity {
    private String name;
    private String type; // EMAIL, CHAT, SMS, SLACK
    private List<Object> members; // hr.Employee refs
    private ArrayDeque<Message> messages;
    private boolean archived;

    public Channel(long id, String name, String type) {
        super(id, "system");
        this.name = name;
        this.type = type;
        this.members = new ArrayList<>();
        this.messages = new ArrayDeque<>();
        this.archived = false;
    }

    public String getName() { return name; }
    public String getType() { return type; }
    public List<Object> getMembers() { return members; }
    public void addMember(Object e) { members.add(e); }
    public ArrayDeque<Message> getMessages() { return messages; }
    public void addMessage(Message m) { messages.add(m); }
    public boolean isArchived() { return archived; }
    public void setArchived(boolean a) { this.archived = a; }
}
