package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qqdp.dto.Result;
import com.qqdp.entity.Shop;
import com.qqdp.entity.ShopType;
import com.qqdp.mapper.ShopMapper;
import com.qqdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.utils.CacheClient;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.HashMap;
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
    @Resource
    private CacheClient cacheClient;

    // JSON工具
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Result queryShopById(Long id) {
        Shop shop = null;

        try {
            // 自己写的
            // 互斥锁解决缓存击穿
            shop = queryWithMutex(id);
            // 逻辑过期解决缓存击穿
            shop = queryWithLogicalExpire(id);

            // 封装的工具类
            // 解决缓存穿透
            shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id,
                    Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
            // 互斥锁解决缓存击穿
            shop = cacheClient.queryWithMutex(RedisConstants.CACHE_SHOP_KEY, id,
                    Shop.class, s -> getById(s), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
            // 逻辑过期解决缓存击穿
            shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id,
                    Shop.class, s -> getById(s), 20L, TimeUnit.SECONDS);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (shop == null) {
            return Result.fail("店铺不存在~");
        }
        return Result.ok(shop);
    }

    /**
     * 利用互斥锁解决缓存击穿问题
     *
     * @param id
     * @return
     */
    private Shop queryWithMutex(Long id) {
        Shop shop = null;

        // 先从 redis 中获取缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(key);
        if (!entries.isEmpty()) {
            // 添加空对象缓存，防止缓存穿透
            if ("false".equals(entries.get("exist"))) {
                return null;
            } else {
                entries.keySet().stream().forEach(s -> {
                    if ("null".equals(entries.get(s))) {
                        entries.put(s, null);
                    }
                });
                shop = BeanUtil.toBean(entries, Shop.class);
            }
        } else {
            try {
                boolean tryLock = tryLock(id);

                // 若未获取到锁，休眠一段时间后重新获取
                if (tryLock) {
                    Thread.sleep(50);
                    return queryWithMutex(id);
                }
                // 获取到锁，查询数据库
                shop = getById(id);
                Map<String, Object> map;

                // 添加空对象缓存，防止缓存穿透
                if (shop == null) {
                    map = new HashMap<>();
                    map.put("exist", "false");
                    stringRedisTemplate.opsForHash().putAll(key, map);
                    stringRedisTemplate.expire(key, RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                } else {
                    map = BeanUtil.beanToMap(shop);
                    for (String s : map.keySet()) {
                        if (!(map.get(s) instanceof String)) {
                            map.put(s, map.get(s) + "");
                        }
                    }
                    stringRedisTemplate.opsForHash().putAll(key, map);
                    stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                // 释放锁
                unlock(id);
            }
        }
        return shop;
    }

    /**
     * 上锁
     *
     * @param id
     * @return
     */
    private boolean tryLock(Long id) {
        // 判断是否存在这个 key
        Boolean flag = stringRedisTemplate.opsForValue()
                .setIfAbsent(RedisConstants.LOCK_SHOP_KEY + id, "1",
                        RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param id
     */
    private void unlock(Long id) {
        stringRedisTemplate.delete(RedisConstants.LOCK_SHOP_KEY + id);
    }

    /**
     * 利用逻辑过期解决缓存击穿问题
     *
     * @param id
     * @return
     */
    private Shop queryWithLogicalExpire(Long id) throws Exception {
        Shop shop = null;

        // 先从 redis 中获取缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String value = stringRedisTemplate.opsForValue().get(key);
        if (!StrUtil.isBlank(value)) {
            RedisData redisData = mapper.readValue(value, RedisData.class);
            // 添加空对象缓存，防止缓存穿透
            if (redisData.getData() != null) {
                if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
                    shop = (Shop) redisData.getData();
                } else {
                    shop = saveWithLogicalExpire(id);
                }
            }
        } else {
            shop = saveWithLogicalExpire(id);
        }
        return shop;
    }

    private Shop saveWithLogicalExpire(Long id) throws Exception {
        Shop shop = null;
        try {
            boolean tryLock = tryLock(id);
            // 未获取到锁，休眠一段时间重新查询
            if (!tryLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            // 拿到锁后要 DoubleCheck 数据是否过期，防止刚才判断时数据还未更新
            // 先从 redis 中获取缓存
            String key = RedisConstants.CACHE_SHOP_KEY + id;
            String value = stringRedisTemplate.opsForValue().get(key);
            if (!StrUtil.isBlank(value)) {
                RedisData redisData = mapper.readValue(value, RedisData.class);
                // 添加空对象缓存，防止缓存穿透
                if (redisData.getData() != null) {
                    if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
                        shop = (Shop) redisData.getData();
                    } else {
                        shop = saveShop2Redis(id);
                    }
                }
            } else {
                shop = saveShop2Redis(id);
            }
        } finally {
            unlock(id);
        }
        return shop;
    }

    private Shop saveShop2Redis(Long id) {
        Shop shop = getById(id);

        // 新开一个线程存 redis
        new Thread(() -> {
            String key = RedisConstants.CACHE_SHOP_KEY + id;
            RedisData data = new RedisData();
            // 逻辑过期时间 20min
            data.setExpireTime(LocalDateTime.now().plusMinutes(20));
            data.setData(shop);
            // 物理过期时间 30min
            try {
                stringRedisTemplate.opsForValue().set(key, mapper.writeValueAsString(data));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }).run();

        return shop;
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺不存在~");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
