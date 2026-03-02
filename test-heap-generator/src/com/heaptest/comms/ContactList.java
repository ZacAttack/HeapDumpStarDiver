package com.heaptest.comms;

import com.heaptest.core.AuditableEntity;
import java.util.LinkedList;

public class ContactList extends AuditableEntity {
    private String name;
    private Object owner; // hr.Employee
    private LinkedList<Object> contacts; // hr.Employee refs
    private String description;
    private boolean shared;

    public ContactList(long id, String name) {
        super(id, "system");
        this.name = name;
        this.contacts = new LinkedList<>();
        this.shared = false;
    }

    public String getName() { return name; }
    public Object getOwner() { return owner; }
    public void setOwner(Object owner) { this.owner = owner; }
    public LinkedList<Object> getContacts() { return contacts; }
    public void addContact(Object e) { contacts.add(e); }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public boolean isShared() { return shared; }
    public void setShared(boolean shared) { this.shared = shared; }
}
