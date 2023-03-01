package com.qqdp.VO;

import com.qqdp.entity.Shop;
import lombok.Data;

@Data
public class ShopVO extends Shop {

    /**
     * 店铺与用户的距离
     */
//    @TableField(exist = false)
    private Double distance;
}
