package com.flinklearn.batch.chapter4;

public class ProductVendorPojo {

    @Override
    public String toString() {
        return "ProductVendorPojo{" +
                "product='" + product + '\'' +
                ", vendor='" + vendor + '\'' +
                '}';
    }

    String product;
    String vendor;

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }
}
