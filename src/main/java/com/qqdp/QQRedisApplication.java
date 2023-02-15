package com.qqdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

// 指定代理可以被 AopContext 类获取
@EnableAspectJAutoProxy(exposeProxy = true)
@MapperScan("com.qqdp.mapper")
@SpringBootApplication
public class QQRedisApplication {

    public static void main(String[] args) {
        SpringApplication.run(QQRedisApplication.class, args);
    }

}
