package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.convert.Convert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.qqdp.dto.Result;
import com.qqdp.entity.Shop;
import com.qqdp.entity.ShopType;
import com.qqdp.mapper.ShopMapper;
import com.qqdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryShopById(Long id) {
        Shop shop;

        // 先从 redis 中获取缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(key);
        if (!entries.isEmpty()) {
            entries.keySet().stream().forEach(s -> {
                if ("null".equals(entries.get(s))) {
                    entries.put(s, null);
                }
            });
            shop = BeanUtil.toBean(entries, Shop.class);
        } else {
            shop = getById(id);
            Map<String, Object> map = BeanUtil.beanToMap(shop);
            for (String s : map.keySet()) {
                if (!(map.get(s) instanceof String)) {
                    map.put(s, map.get(s) + "");
                }
            }
            stringRedisTemplate.opsForHash().putAll(key, map);
        }
        stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
    }
}
