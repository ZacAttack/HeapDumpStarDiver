// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.inventory;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.Address;
import java.util.ArrayList;
import java.util.List;

public class Supplier extends AuditableEntity {
    private String name;
    private String contactEmail;
    private Address address;
    private List<Product> products;
    private float rating;
    private String phone;

    public Supplier(long id, String name, String contactEmail) {
        super(id, "system");
        this.name = name;
        this.contactEmail = contactEmail;
        this.products = new ArrayList<>();
        this.rating = 0.0f;
    }

    public String getName() { return name; }
    public String getContactEmail() { return contactEmail; }
    public Address getAddress() { return address; }
    public void setAddress(Address addr) { this.address = addr; }
    public List<Product> getProducts() { return products; }
    public void addProduct(Product p) { products.add(p); }
    public float getRating() { return rating; }
    public void setRating(float rating) { this.rating = rating; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
}
