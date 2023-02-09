package com.qqdp.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qqdp.dto.Result;
import com.qqdp.entity.ShopType;
import com.qqdp.mapper.ShopTypeMapper;
import com.qqdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    // JSON工具
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Result queryTypeList() {
        List<ShopType> typeList;

        // 先从 redis 中获取缓存
        String key = RedisConstants.CACHE_SHOP_KEY + "typeList";
        List<String> range = stringRedisTemplate.opsForList().range(key, 0, -1);
        if (!range.isEmpty()) {
            typeList = range.stream().map(s -> {
                try {
                    return mapper.readValue(s, ShopType.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toList());
        } else {
            typeList = query().orderByAsc("sort").list();
            range = typeList.stream().map(shopType -> {
                try {
                    return mapper.writeValueAsString(shopType);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toList());
            stringRedisTemplate.opsForList().leftPushAll(key, range);
            stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }
        return Result.ok(typeList);
    }
}
