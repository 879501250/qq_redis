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
import com.qqdp.utils.*;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
            loadFollows(user.getId());

            // 登录成功，用户自动签到
            sign(user.getId());
        } catch (JsonProcessingException e) {
            return Result.fail(e.getMessage());
        }

        return Result.ok(token);
    }

    // 用户签到
    private void sign(Long id) {
        // 获取当前日期
        LocalDateTime now = LocalDateTime.now();
        // 获取 key
        String key = getUserSignKey(id, now);
        // 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
    }

    /**
     * 获取用户签到 key
     *
     * @param id
     * @param time
     * @return
     */
    private String getUserSignKey(Long id, LocalDateTime time) {
        String prefixKey = RedisConstants.USER_SIGN_KEY + id;
        // 拼接 key
        return prefixKey + time.format(DateTimeFormatter.ofPattern(":yyyy:MM"));
    }

    // 数据预热，加载用户关注列表
    private void loadFollows(Long userId) {
        String followKey = RedisConstants.USER_FOLLOWS_KEY + userId;
        stringRedisTemplate.delete(followKey);
        List<String> user_ids = followService.query().eq("user_id", userId).list()
                .stream().map(follow -> follow.getFollowUserId().toString()).collect(Collectors.toList());
        String[] users;
        if (user_ids.size() > 0) {
            users = user_ids.toArray(new String[user_ids.size()]);
        } else {
            users = new String[]{"-1"};
        }
        stringRedisTemplate.opsForSet().add(followKey, users);
    }

    @Override
    public Result logout(String token) {
        if (!StrUtil.isBlank(token)) {
            String key = RedisConstants.LOGIN_USER_KEY + token;
            stringRedisTemplate.delete(key);
        }
        return Result.ok("登出成功~");
    }

    @Override
    public Result me() {
        // 获取用户信息
        UserDTO user = UserHolder.getUser();

        // 获取用户当月连续签到天数
        LocalDateTime now = LocalDateTime.now();
        String key = getUserSignKey(user.getId(), now);
        int dayOfMonth = now.getDayOfMonth();
        // 获取本月截止今天为止的所有的签到记录，返回的是一个十进制的数字
        // 因为底层是字节存储，位数一定是8的倍数，因此要排除补位的数字，只取前几位
        // BITFIELD sign:5:202203 GET u14 0
        List<Long> record = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (record == null || record.isEmpty()) {
            // 没有任何签到结果
            user.setSignCount(0);
        } else {
            Long num = record.get(0);
            int count = 0;
            // 判断是否签到
            // 让这个数字与1做与运算，得到数字的最后一个bit位
            while ((num & 1) != 0) {
                // 如果不为0，说明已签到，计数器+1
                count++;
                // 把数字右移一位，抛弃最后一个bit位，继续下一个bit位
                num >>>= 1;
            }
            user.setSignCount(count);
        }

        return Result.ok(user);
    }
}
