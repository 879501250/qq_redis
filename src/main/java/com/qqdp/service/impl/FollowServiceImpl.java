package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.qqdp.dto.Result;
import com.qqdp.dto.UserDTO;
import com.qqdp.entity.Follow;
import com.qqdp.mapper.FollowMapper;
import com.qqdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.service.IUserService;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private IUserService userService;
    @Resource
    StringRedisTemplate stringRedisTemplate;

    /**
     * 是否关注某博主
     *
     * @param id
     * @return
     */
    @Override
    public Result isFollow(Long id) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.ok(false);
        }
        String followKey = RedisConstants.USER_FOLLOWS_KEY + user.getId();
        // 判断是否关注
        Boolean aBoolean = stringRedisTemplate.opsForSet().isMember(followKey, id.toString());
        return Result.ok(aBoolean);
    }

    /**
     * 关注/取关某博主
     *
     * @param id
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long id, Boolean isFollow) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录~");
        }
        String followKey = RedisConstants.USER_FOLLOWS_KEY + user.getId();
        // 1.判断到底是关注还是取关
        Boolean aBoolean = stringRedisTemplate.opsForSet().isMember(followKey, id.toString());
        if (!BooleanUtil.isTrue(aBoolean)) {
            // 2.关注，新增数据
            Follow follow = new Follow();
            follow.setFollowUserId(id);
            follow.setUserId(user.getId());
            follow.setCreateTime(LocalDateTime.now());
            boolean isSuccess = save(follow);
            if (!isSuccess) {
                return Result.fail("关注失败~");
            }
            // 把关注用户的id，放入redis的set集合 sadd userId followerUserId
            stringRedisTemplate.opsForSet().add(followKey, id.toString());
        } else {
            // 3.取关，删除 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", user.getId()).eq("follow_user_id", id));
            if (!isSuccess) {
                return Result.fail("取消关注失败~");
            }
            // 把关注用户的id从Redis集合中移除
            stringRedisTemplate.opsForSet().remove(followKey, id.toString());

            // TODO：取消关注后要将要将博主的所有推送的博客 id 从 redis 中用户关注博主的博客列表中删除
        }
        return Result.ok("关注成功~");
    }

    /**
     * 查询当前用户与指定用户之间的共同关注列表
     *
     * @param id
     * @return
     */
    @Override
    public Result common(Long id) {
        // 1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录~");
        }
        Long userId = user.getId();
        String key = RedisConstants.USER_FOLLOWS_KEY + userId;
        String key2 = RedisConstants.USER_FOLLOWS_KEY + id;

        // 实际业务一般都有数据预热，无需这步
        Boolean aBoolean = stringRedisTemplate.hasKey(key2);
        if (BooleanUtil.isFalse(aBoolean)) {
            setFollows(id);
        }

        // 2.求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        // 3.解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 4.查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(result -> BeanUtil.copyProperties(result, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }

    // 缓存指定用户的关注列表
    private void setFollows(Long id) {
        String followKey = RedisConstants.USER_FOLLOWS_KEY + id;
        stringRedisTemplate.delete(followKey);
        List<String> user_ids = query().eq("user_id", id).list().stream()
                .map(follow -> follow.getFollowUserId().toString()).collect(Collectors.toList());
        String[] users;
        if (user_ids.size() > 0) {
            users = user_ids.toArray(new String[user_ids.size()]);
        } else {
            users = new String[]{"-1"};
        }
        stringRedisTemplate.opsForSet().add(followKey, users);
    }
}
