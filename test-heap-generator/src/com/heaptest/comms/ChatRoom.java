package com.heaptest.comms;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;

public class ChatRoom extends AuditableEntity {
    private String name;
    private List<Object> participants; // hr.Employee refs
    private ArrayDeque<Message> recentMessages;
    private int maxHistory;
    private boolean isPrivate;
    private String topic;

    public ChatRoom(long id, String name, int maxHistory) {
        super(id, "system");
        this.name = name;
        this.maxHistory = maxHistory;
        this.participants = new ArrayList<>();
        this.recentMessages = new ArrayDeque<>();
        this.isPrivate = false;
    }

    public String getName() { return name; }
    public List<Object> getParticipants() { return participants; }
    public void addParticipant(Object e) { participants.add(e); }
    public ArrayDeque<Message> getRecentMessages() { return recentMessages; }
    public void addMessage(Message m) {
        recentMessages.add(m);
        while (recentMessages.size() > maxHistory) recentMessages.pollFirst();
    }
    public int getMaxHistory() { return maxHistory; }
    public boolean isPrivate() { return isPrivate; }
    public void setPrivate(boolean p) { this.isPrivate = p; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
}
