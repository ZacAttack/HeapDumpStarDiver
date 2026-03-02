package com.heaptest.core;

import java.util.HashSet;
import java.util.Set;

public abstract class TaggableEntity extends AuditableEntity {
    protected Set<String> tags;

    public TaggableEntity(long id, String createdBy) {
        super(id, createdBy);
        this.tags = new HashSet<>();
    }

    public void addTag(String tag) { tags.add(tag); }
    public void removeTag(String tag) { tags.remove(tag); }
    public Set<String> getTags() { return tags; }
}
