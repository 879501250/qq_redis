package com.qqdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan("com.qqdp.mapper")
@SpringBootApplication
public class QQRedisApplication {

    public static void main(String[] args) {
        SpringApplication.run(QQRedisApplication.class, args);
    }

}
