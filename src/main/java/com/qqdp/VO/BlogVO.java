package com.qqdp.VO;

import com.qqdp.entity.Blog;
import lombok.Data;

@Data
public class BlogVO extends Blog {
    /**
     * 用户图标
     */
    private String icon;
    /**
     * 用户姓名
     */
    private String name;
    /**
     * 是否点赞过了
     */
    private Boolean isLike;
}
