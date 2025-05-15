package com.userportrait.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimOrderCompare {
    private Integer sku_id;
    private String tm_name;
    private String sku_price;
    private String order_category;
}
