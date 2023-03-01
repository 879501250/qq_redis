package com.qqdp.dto;

import lombok.Data;

@Data
public class UserDTO {
    private Long id;
    private String nickName;
    private String icon;

    // 当月连续签到天数
    private Integer signCount;
}
