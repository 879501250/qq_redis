package com.qqdp.utils;

import cn.hutool.core.util.*;
import cn.hutool.json.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Redis 缓存工具
 * <p>
 * 使用 @Component 表示将此类交由 IOC 管理，因此可注入其他类中直接使用，
 * 且在容器启动时将根据构造器自动创建时构造器所需要的参数将自动传入，
 * stringRedisTemplate 已在 IOC 容器中，因此可以达到自动注入的目的，
 * 相当于直接在 stringRedisTemplate 上加 @Resource。
 * <p>
 * 方法不写成 static 是因为静态方法只能调用静态属性，因此需将 stringRedisTemplate
 * 也设置成 static，若设置成静态属性，将无法通过构造器或注解注入，需要通过
 * set方法注入、@PostConstruct、MethodInvokingFactoryBean等方法注入，
 * 需要重新配置 stringRedisTemplate，比较麻烦。
 */
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    // NULL 数据缓存过期时间 - 2分钟
    private static final long CACHE_NULL_TTL = 2L;
    // redis 锁前缀
    private static final String LOCK_KEY = "lock:";

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 设置 String 类型的 key-value 及数据过期时间，参考 Redis Setex 命令
     * <br>
     * 将任意 Java 对象序列化为 json 并存储在 string 类型的 key 中，并且可以设置 TTL 过期时间
     *
     * @param key
     * @param value
     * @param time  过期时间
     * @param unit  过期时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 设置 String 类型的 key-value 及逻辑过期时间，数据本身不会过期
     * <br>
     * 将任意 Java 对象序列化为 json 并存储在 string 类型的 key 中，
     * 并且可以设置逻辑过期时间，用于处理缓存击穿问题
     *
     * @param key
     * @param value
     * @param time  逻辑过期时间
     * @param unit  逻辑过期时间单位
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 将数据进行封装（添加逻辑过期时间属性），设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 根据指定的 key 查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     *
     * @param keyPrefix  redis 中缓存的 key 前缀
     * @param id         数据 id
     * @param type       数据类型
     * @param dbFallback 若缓存不存在，查询数据库的回调函数
     * @param time       缓存过期时间
     * @param unit       缓存过期时间单位
     * @param <R>
     * @param <ID>
     * @return 查询到的数据
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从 redis 中查询数据缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (json != null) {
            // 判断命中的是否是空值
            if (StrUtil.isBlank(json)) {
                return null;
            } else {
                return JSONUtil.toBean(json, type);
            }
        }

        // 3.不存在，根据 id 查询数据库
        R result = dbFallback.apply(id);
        // 4.判断是否存在
        if (result == null) {
            // 将空值写入 redis
            set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
        } else {
            // 5.存在，写入 redis
            set(key, result, time, unit);
        }

        return result;
    }

    // 获取锁
    private boolean tryLock(String key) {
        // 相当于 setnx 指令，只有 key 不存在的时候才设置
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    /**
     * 根据指定的 key 查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     *
     * @param keyPrefix  redis 中缓存的 key 前缀
     * @param id         数据 id
     * @param type       数据类型
     * @param dbFallback 若缓存不存在，查询数据库的回调函数
     * @param time       缓存过期时间
     * @param unit       缓存过期时间单位
     * @param <R>
     * @param <ID>
     * @return 查询到的数据
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从 redis 中查询数据缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        // 热点数据一般会提前预热，缓存在 redis 中，且采用逻辑过期方案缓存不会自动失效，
        // 因此一定存在缓存，若不存在则数据 id 有问题
        if (StrUtil.isBlank(json)) {
            // 3.不存在，直接返回
            return null;
        }
        // 4.命中，需要先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
//        R result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 5.判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            // 5.1.未过期，直接返回数据信息
//            return result;
//        }
        R result = checkIsExpired(json, type);
        if (result != null) {
            return result;
        }

        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_KEY + key;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock) {
            // 成功，但需要二次校验
            result = checkIsExpired(stringRedisTemplate.opsForValue().get(key), type);
            if (result == null) {
                // 另外开一个线程更新缓存
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    try {
                        // 查询数据库
                        R newR = dbFallback.apply(id);
                        // 重建缓存
                        this.setWithLogicalExpire(key, newR, time, unit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        unlock(lockKey);
                    }
                });
            }
        }
        // 6.4.返回过期的商铺信息，因此不能保证数据的一致性
        return result;
    }

    /**
     * 判断数据是否逻辑过期
     *
     * @param json 数据的 json 格式
     * @param type 数据类型
     * @param <R>
     * @return
     */
    private <R> R checkIsExpired(String json, Class<R> type) {
        if (StrUtil.isNotBlank(json)) {
            // 需要先把json反序列化为对象
            RedisData redisData = JSONUtil.toBean(json, RedisData.class);
            R result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            LocalDateTime expireTime = redisData.getExpireTime();
            // 判断是否过期
            if (expireTime.isAfter(LocalDateTime.now())) {
                // 未过期，直接返回数据信息
                return result;
            }
        }
        return null;
    }

    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type,
                                    Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从 redis 中查询数据缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的值是否是空值，解决缓存击穿问题
        if (json != null) {
            // 返回一个错误信息
            return null;
        }
        // 4.实现缓存重构
        // 4.1 获取互斥锁
        String lockKey = LOCK_KEY + key;
        R result = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2 判断否获取成功
            if (!isLock) {
                //4.3 失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 4.4 根据id查询数据库
            result = dbFallback.apply(id);
            // 5.不存在，返回错误
            if (result == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            // 6.写入redis
            set(key, result, time, unit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }
        return result;
    }
}
