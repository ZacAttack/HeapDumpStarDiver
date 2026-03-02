package com.heaptest.inventory;

import com.heaptest.core.TaggableEntity;

public class Product extends TaggableEntity {
    private String name;
    private String sku;
    private double price;
    private float weight;
    private Category category;
    private String description;
    private boolean active;

    public Product(long id, String name, String sku, double price) {
        super(id, "system");
        this.name = name;
        this.sku = sku;
        this.price = price;
        this.weight = 0.0f;
        this.active = true;
    }

    public String getName() { return name; }
    public String getSku() { return sku; }
    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
    public float getWeight() { return weight; }
    public void setWeight(float weight) { this.weight = weight; }
    public Category getCategory() { return category; }
    public void setCategory(Category c) { this.category = c; }
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
}
