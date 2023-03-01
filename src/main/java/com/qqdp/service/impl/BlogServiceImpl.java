package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.servlet.ServletUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.qqdp.VO.BlogVO;
import com.qqdp.dto.Result;
import com.qqdp.dto.ScrollResult;
import com.qqdp.dto.UserDTO;
import com.qqdp.entity.Blog;
import com.qqdp.entity.User;
import com.qqdp.mapper.BlogMapper;
import com.qqdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.service.IFollowService;
import com.qqdp.service.IUserService;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.SystemConstants;
import com.qqdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
    private IFollowService followService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 保存博客信息
     *
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
        LocalDateTime now = LocalDateTime.now();
        blog.setCreateTime(now);
        // 保存探店博文
        boolean save = save(blog);
        if (!save) {
            return Result.fail("保存失败~");
        }

        long l = LocalDateTimeUtil.toEpochMilli(now);
        // 查询粉丝 select * from tb_follow where follow_user_id = ?
        followService.query().eq("follow_user_id", user.getId()).list().forEach(follow -> {
            // 推送给粉丝，value 为博客 id
            String blogKey = RedisConstants.BLOG_USER_KEY + follow.getUserId();
            stringRedisTemplate.opsForZSet().add(blogKey, blog.getId().toString(), l);
        });

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
     *
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
     *
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
     *
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();

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
            if (user != null) {
                // 判断是否点赞
                blog.setIsLike(isLike(RedisConstants.BLOG_LIKED_KEY + blog.getId(), user.getId().toString()));
            }
            return blog;
        }).collect(Collectors.toList());
        return Result.ok(blogVOList);
    }

    /**
     * 查询指定博客详细信息
     *
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();

        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在~");
        }

        BlogVO blogVO = BeanUtil.copyProperties(blog, BlogVO.class);
        String value;
        setBlogger(blogVO);
        if (user != null) {
            // 判断是否点赞
            blogVO.setIsLike(isLike(RedisConstants.BLOG_LIKED_KEY + blog.getId(), user.getId().toString()));
            value = user.getId().toString();
        } else {
            // 若未登录，取请求 ip
            RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
            HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
            value = ServletUtil.getClientIP(request);
        }

        // 设置博客浏览量
        blogVO.setView(getBlogView(blogVO.getId(), value));

        return Result.ok(blogVO);
    }

    /**
     * 设置并获取博客浏览量
     *
     * @param blogId 博客 id
     * @param value  若已登录者为用户 id，若未登录则为主机 ip
     * @return
     */
    private Long getBlogView(Long blogId, String value) {
        String viewKey = RedisConstants.BLOG_VIEW_KEY + blogId;
        // 先增加浏览量
        stringRedisTemplate.opsForHyperLogLog().add(viewKey, value);
        // 获取浏览量
        Long size = stringRedisTemplate.opsForHyperLogLog().size(viewKey);
        return size == null ? 0 : size;
    }

    /**
     * 查询博客的点赞用户，筛选前几个
     *
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

    /**
     * 查询用户关注博主的博客
     *
     * @param max    时间戳最大值，第一次查询时为当前时间戳，之后均为上次查询的最小值，
     * @param offset 偏移量，因为是从上次查询的最小时间戳开始查询，因此要跳过上次已查询的数据，
     *               第一次为0（因为无上次查询记录），之后均为上次查询时与最小时间戳相同的查询个数
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            Result.fail("请先登录~");
        }

        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        // 从 max 到 min，跳过 offset 个后，取 count 个
        String blogKey = RedisConstants.BLOG_USER_KEY + user.getId();
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(blogKey, 0, max, offset, SystemConstants.DEFAULT_PAGE_SIZE);
        // 3.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 4.解析数据：准备 ids 和下次查询的 max、offset
        // 如 5，4，2，2，2 得出 max 为 2，offset 为 3
        List<Long> ids = new ArrayList<>(typedTuples.size());
        offset = 0;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 4.1.获取id
            ids.add(Long.valueOf(typedTuple.getValue()));
            // 4.2.获取分数(时间戳）
            long time = typedTuple.getScore().longValue();
            if (time == max) {
                offset++;
            } else {
                max = time;
                offset = 1;
            }
        }
        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        String userId = user.getId().toString();
        List<BlogVO> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")")
                .list().stream().map(blog -> {
                    BlogVO blogVO = BeanUtil.copyProperties(blog, BlogVO.class);
                    // 5.1.设置 blog 有关的用户信息
                    setBlogger(blogVO);
                    // 5.2.查询 blog 是否被点赞
                    blogVO.setIsLike(
                            isLike(RedisConstants.BLOG_LIKED_KEY + blog.getId(), userId));
                    return blogVO;
                }).collect(Collectors.toList());

        // 6.封装并返回
        ScrollResult result = new ScrollResult();
        result.setList(blogs);
        result.setOffset(offset);
        result.setMinTime(max);

        return Result.ok(result);
    }

    // 设置博主信息
    private void setBlogger(BlogVO blogVO) {
        User user = userService.getById(blogVO.getUserId());
        blogVO.setName(user.getNickName());
        blogVO.setIcon(user.getIcon());
    }
}
