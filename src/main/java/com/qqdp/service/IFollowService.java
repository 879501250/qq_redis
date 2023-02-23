package com.qqdp.service;

import com.qqdp.dto.Result;
import com.qqdp.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IFollowService extends IService<Follow> {

    Result isFollow(Long id);

    Result follow(Long id, Boolean isFollow);

    Result common(Long id);
}
