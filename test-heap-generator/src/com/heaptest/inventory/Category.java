// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;
import java.util.ArrayList;
import java.util.List;

public class Category extends AuditableEntity {
    private String name;
    private Category parentCategory; // self-reference!
    private List<Category> subCategories;
    private List<Product> products;
    private int depth;

    public Category(long id, String name) {
        super(id, "system");
        this.name = name;
        this.subCategories = new ArrayList<>();
        this.products = new ArrayList<>();
        this.depth = 0;
    }

    public String getName() { return name; }
    public Category getParentCategory() { return parentCategory; }
    public void setParentCategory(Category parent) {
        this.parentCategory = parent;
        this.depth = parent.depth + 1;
        parent.subCategories.add(this);
    }
    public List<Category> getSubCategories() { return subCategories; }
    public List<Product> getProducts() { return products; }
    public void addProduct(Product p) { products.add(p); }
    public int getDepth() { return depth; }
}
