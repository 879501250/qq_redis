package com.qqdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 利用 Redis 实现分布式锁，核心是使用 setnx 判断是否插入值成功
 * <br>
 * 可见性：多个线程都能看到相同的结果，注意：这个地方说的可见性并不是并发编程中指的内存可见性，
 * 只是说多个进程之间都能感知到变化的意思
 * <br>
 * 互斥：互斥是分布式锁的最基本的条件，使得程序串行执行
 * <br>
 * 高可用：程序不易崩溃，时时刻刻都保证较高的可用性
 * <br>
 * 高性能：由于加锁本身就让性能降低，所有对于分布式锁本身需要他就较高的加锁性能和释放锁性能
 * <br>
 * 安全性：安全也是程序中必不可少的一环
 */
public class SimpleRedisLock implements ILock {

    // 分布式锁标志的 id 前缀，用于区分分布式系统不同的主机
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    // 分布式锁的 key 前缀
    private String KEY_PREFIX = "lock:";

    private StringRedisTemplate stringRedisTemplate;
    // 分布式锁的 key
    private String key;

    // lua 脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    // 加载 lua 脚本
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        // ClassPathResource 可以获取 class 路径下的资源
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String key) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.key = KEY_PREFIX + key;
    }

    @Override
    public boolean tryLock(long timeSec) {
        // 获取当前线程 id
        long id = Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(
                key, ID_PREFIX + id, timeSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        long id = Thread.currentThread().getId();
        // 1.若由于业务阻塞，执行到这步时锁可能已经过期自动释放了，且此时其他线程尝试获取锁成功
        // 因此在删除锁前需要判断锁标志是否一致，防止误删
        // 2.由于判断锁标志是否一致和删除锁是两个步骤，不是原子性，因此可能存在判断锁标志为真后，
        // 还未及时删除当前锁，锁已超时生效，由其他线程获取锁，此时再删除还是可能误删，
        // 因此使用 lua 脚本解决多条命令原子性问题
//        if ((ID_PREFIX + id).equals(stringRedisTemplate.opsForValue().get(key))) {
//            stringRedisTemplate.delete(key);
//        }
        // 执行 lua 脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(key),
                ID_PREFIX + id);
    }
}
