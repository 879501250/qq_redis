package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qqdp.dto.LoginFormDTO;
import com.qqdp.dto.Result;
import com.qqdp.dto.UserDTO;
import com.qqdp.entity.User;
import com.qqdp.mapper.UserMapper;
import com.qqdp.service.IFollowService;
import com.qqdp.service.IUserService;
import com.qqdp.utils.PasswordEncoder;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.RegexUtils;
import com.qqdp.utils.SystemConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    // JSON工具
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式不正确~");
        }
        String code = RandomUtil.randomNumbers(6);

        // 保存到 session 中
//        session.setAttribute("phone:" + phone, code);
        // 保存到 redis 中
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code,
                RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.debug("[" + phone + "]发送短信验证码成功，验证码：" + code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (StringUtils.isEmpty(phone)) {
            return Result.fail("请输入手机号~");
        }

        // 验证码登录
        if (loginForm.getCode() != null) {
            String code;

            // 从 session 中取验证码
//            code = (String) session.getAttribute(
//                    RedisConstants.LOGIN_CODE_KEY + phone);
            // 从 redis 中取验证码
            code = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);

            if (StrUtil.isBlank(code)) {
                return Result.fail("请用当前手机号重新获取验证码~");
            }
            if (!code.equals(loginForm.getCode())) {
                return Result.fail("验证码错误~");
            }
        }

        User user = query().eq("phone", phone).one();
        if (user != null) {
            // 密码登录
            if (loginForm.getPassword() != null
                    && PasswordEncoder.matches(user.getPassword(), loginForm.getPassword())) {
                return Result.fail("密码错误~");
            }
        } else {
            // 创建用户
            user = new User();
            user.setPhone(phone);
            if (loginForm.getPassword() != null) {
                user.setPassword(PasswordEncoder.encode(loginForm.getPassword()));
            }
            // 默认用户名
            user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
            save(user);
        }

        // 保存到 session
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        // 保存到 redis
        String token = UUID.randomUUID().toString(true);
        try {
            // 将对象转化为 json 字符串进行存储
            String value = mapper.writeValueAsString(BeanUtil.copyProperties(user, UserDTO.class));
            stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_USER_KEY + token,
                    value,
                    RedisConstants.LOGIN_USER_TTL, TimeUnit.SECONDS);

            // 登录成功，将用户关注列表保存到 redis 中
            String followKey = RedisConstants.USER_FOLLOWS + user.getId();
            stringRedisTemplate.delete(followKey);
            List<String> user_ids = followService.query().eq("user_id", user.getId()).list()
                    .stream().map(follow -> follow.getFollowUserId().toString()).collect(Collectors.toList());
            if (user_ids.size() > 0) {
                String[] users = user_ids.toArray(new String[user_ids.size()]);
                stringRedisTemplate.opsForSet().add(followKey, users);
            }
        } catch (JsonProcessingException e) {
            return Result.fail(e.getMessage());
        }

        return Result.ok(token);
    }

    @Override
    public Result logout(String token) {
        if (!StrUtil.isBlank(token)) {
            String key = RedisConstants.LOGIN_USER_KEY + token;
            stringRedisTemplate.delete(key);
        }
        return Result.ok("登出成功~");
    }
}
