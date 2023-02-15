package com.qqdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Redis 实现全局唯一 Id，类似雪花算法
 * <p></p>
 * 问题分析：
 * <br>
 * 1) 现在的服务基本是分布式、微服务形式的，而且大数据量也导致分库分表的产生，
 * 对于水平分表就需要保证表中 id 的全局唯一性。
 * <br>
 * 2) 对于 MySQL 而言，一个表中的主键 id 一般使用自增的方式，但是如果进行水平分表之后，
 * 多个表中会生成重复的 id 值。那么如何保证水平分表后的多张表中的 id 是全局唯一性的呢？
 * <br>
 * 3) 如果还是借助数据库主键自增的形式，那么可以让不同表初始化一个不同的初始值，
 * 然后按指定的步长进行自增。例如有3张拆分表，初始主键值为1，2，3，自增步长为3。
 * <br>
 * 4) 当然也有人使用 UUID 来作为主键，但是 UUID 生成的是一个无序的字符串，
 * 对于 MySQL 推荐使用增长的数值类型值作为主键来说不适合。
 * <br>
 * 5) 也可以使用 Redis 的自增原子性来生成唯一 id，但是这种方式业内比较少用。
 * <p></p>
 * Id 组成部分：
 * <br>
 * 符号位：1bit，永远为0
 * <br>
 * 时间戳：31bit，以秒为单位，可以使用69年
 * <br>
 * 序列号：32bit，秒内的计数器，支持每秒产生2^32个不同ID
 */
@Component
public class RedisIdWorker {
    /**
     * 开始时间戳
     * 2022-01-01 00:00:00
     */
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    /**
     * 序列号的位数，代表每秒内可产生最大序列号，即 2^32 - 1
     */
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2.生成序列号
        // 2.1.获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2.自增长
        // 采取这种格式的 key，后期方便统计某年某月某天的总量
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }
}
