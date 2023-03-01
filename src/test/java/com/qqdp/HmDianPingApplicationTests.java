package com.qqdp;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@SpringBootTest
@RunWith(SpringRunner.class)
public class HmDianPingApplicationTests {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testHyperLogLog(){
        // 准备数据，装用户信息
        String[] users = new String[1000];
        // 数组角标
        int index = 0;
        for (int i = 1; i < 1000000; i++) {
            // 赋值
            users[index++]="user_"+i;
            // 每 1000 条发送一次
            if(i%1000==0){
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1",users);
            }
        }
        // 统计数量
        System.out.println(stringRedisTemplate.opsForHyperLogLog().size("hll1"));
    }

}
