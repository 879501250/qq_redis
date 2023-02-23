package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.qqdp.VO.BlogVO;
import com.qqdp.dto.Result;
import com.qqdp.dto.UserDTO;
import com.qqdp.entity.Blog;
import com.qqdp.entity.User;
import com.qqdp.mapper.BlogMapper;
import com.qqdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.service.IUserService;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.SystemConstants;
import com.qqdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 保存博客信息
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录~");
        }
        blog.setUserId(user.getId());
        // 保存探店博文
        save(blog);
        // 返回id
        return Result.ok(blog.getId());
    }

    /**
     * 判断用户是否点赞该博客
     *
     * @param likeKey redis 中博客点赞 key
     * @param userId  用户 id
     * @return
     */
    private boolean isLike(String likeKey, String userId) {
        Double score = stringRedisTemplate.opsForZSet().score(likeKey, userId);
        return score != null;
    }

    /**
     * 点赞/取消点赞博客
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录~");
        }

        String likeKey = RedisConstants.BLOG_LIKED_KEY + id;
        String userId = user.getId().toString();
        // 判断是否点赞
        boolean like = isLike(likeKey, userId);
        // 非核心业务，如点赞数、浏览量等无需添加锁等，数据出错影响不大
        if (!like) {
            stringRedisTemplate.opsForZSet().add(likeKey, userId, System.currentTimeMillis());
            // 修改点赞数量
            update().setSql("liked = liked + 1").eq("id", id).update();
        } else {
            stringRedisTemplate.opsForZSet().remove(likeKey, userId);
            // 修改点赞数量
            update().setSql("liked = liked - 1").eq("id", id).update();
        }
        return Result.ok();
    }

    /**
     * 查询我的博客
     * @param current
     * @return
     */
    @Override
    public Result queryMyBlog(Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            Result.fail("请先登录~");
        }
        // 根据用户查询
        Page<Blog> page = query().eq("user_id", user.getId())
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    /**
     * 查询热门博客，根据点赞数排序
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        String userId = user.getId().toString();

        // 根据点赞数排名
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        List<BlogVO> blogVOList = records.stream().map(record -> {
            BlogVO blog = BeanUtil.copyProperties(record, BlogVO.class);
            setBlogger(blog);
            if (StrUtil.isNotBlank(userId)) {
                // 判断是否点赞
                blog.setIsLike(isLike(RedisConstants.BLOG_LIKED_KEY + blog.getId(), userId));
            }
            return blog;
        }).collect(Collectors.toList());
        return Result.ok(blogVOList);
    }

    /**
     * 查询指定博客详细信息
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        String userId = user.getId().toString();

        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在~");
        }

        BlogVO blogVO = BeanUtil.copyProperties(blog, BlogVO.class);
        setBlogger(blogVO);
        if (StrUtil.isNotBlank(userId)) {
            // 判断是否点赞
            blogVO.setIsLike(isLike(RedisConstants.BLOG_LIKED_KEY + blog.getId(), userId));
        }
        return Result.ok(blogVO);
    }

    /**
     * 查询博客的点赞用户，筛选前几个
     * @param id
     * @return
     */
    @Override
    public Result likesBlog(Long id) {
        String likeKey = RedisConstants.BLOG_LIKED_KEY + id;
        // 1.查询 top6 的点赞用户 zrange key 0 5
        Set<String> range = stringRedisTemplate.opsForZSet().
                range(likeKey, 0, SystemConstants.DEFAULT_PAGE_SIZE);
        if (range == null || range.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 2.解析出其中的用户id
        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        // 将 id 以逗号拼接
        String idStr = StrUtil.join(",", ids);
        // 3.根据用户id查询用户 WHERE id IN ( 5 , 1 , 6) ORDER BY FIELD(id, 5, 1, 6)
        List<UserDTO> userDTOS = userService.query()
                // WHERE id IN ( 5 , 1 , 6)
                .in("id", ids)
                // 在 sql 语句最后拼接上 ORDER BY FIELD(id, 5, 1, 6)
                // 表示按照某字段指定排序
                .last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOS);
    }

    // 设置博主信息
    private void setBlogger(BlogVO blogVO) {
        User user = userService.getById(blogVO.getUserId());
        blogVO.setName(user.getNickName());
        blogVO.setIcon(user.getIcon());
    }
}
