package com.qqdp.utils;

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qqdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.concurrent.TimeUnit;

/**
 * 自动刷新 token 有效时间的拦截器
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    // JSON工具
    private static final ObjectMapper mapper = new ObjectMapper();

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        Object user;

        // 从 session 中拿
//        HttpSession session = request.getSession();
//        user = session.getAttribute("user");
        // 从 redis 中拿
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return true;
        }
        String key = RedisConstants.LOGIN_USER_KEY + token;
        String userStr = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(userStr)) {
            return true;
        }
        user = mapper.readValue(userStr, UserDTO.class);

        if (user != null) {
            // 将用户信息保存在 ThreadLocal
            UserHolder.saveUser((UserDTO) user);
            // 重新设置 redis 中 token 的过期时间
            stringRedisTemplate.expire(key, RedisConstants.LOGIN_USER_TTL, TimeUnit.SECONDS);
        }

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 要及时移除 ThreadLocal，防止内存泄漏
        UserHolder.removeUser();
    }
}
