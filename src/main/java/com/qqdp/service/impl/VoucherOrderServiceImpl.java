package com.qqdp.service.impl;

import com.qqdp.dto.Result;
import com.qqdp.entity.SeckillVoucher;
import com.qqdp.entity.VoucherOrder;
import com.qqdp.mapper.VoucherOrderMapper;
import com.qqdp.service.ISeckillVoucherService;
import com.qqdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.utils.CacheClient;
import com.qqdp.utils.RedisConstants;
import com.qqdp.utils.RedisIdWorker;
import com.qqdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        SeckillVoucher voucher = cacheClient.queryWithPassThrough(
                RedisConstants.CACHE_SECKILL_VOUCHER_KEY, voucherId, SeckillVoucher.class,
                id -> {
                    SeckillVoucher result = seckillVoucherService.getById(id);
                    if (result != null) {
                        // 设置库存缓存
                        stringRedisTemplate.opsForValue()
                                .setIfAbsent(stockKey, result.getStock().toString());
                    }
                    return result;
                },
                RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (voucher == null) {
            return Result.fail("优惠券不存在~");
        }
        // 2.判断秒杀是否开始
        LocalDateTime now = LocalDateTime.now();
        if (voucher.getBeginTime().isAfter(now)) {
            return Result.fail("秒杀活动还未开始~");
        }
        if (voucher.getEndTime().isBefore(now)) {
            return Result.fail("秒杀活动已结束~");
        }
        // 3.判断库存是否充足
        String stock = stringRedisTemplate.opsForValue().get(stockKey);
        if (Integer.parseInt(stock) < 1) {
            return Result.fail("库存不足~");
        }
        // 如果锁加在方法内部，因为该方法被 spring 的事务控制，
        // 会导致当前方法事务还没有提交，但是锁已经释放也会导致问题，
        // 所以选择将当前方法整体包裹起来，确保事务不会出现问题
        Long userId = UserHolder.getUser().getId();
        synchronized (voucherId + ":" + userId) {
            // 调用的方法，其实是this.的方式调用的，事务想要生效，
            // 还得利用代理来生效，所以这个地方，我们需要获得原始的事务对象，来操作事务
            // return this.createVoucherOrder(voucherId, userId, stockKey);
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, userId, stockKey);
        }
    }

    /**
     * 创建秒杀订单
     *
     * @param voucherId
     * @param userId
     * @param stockKey
     * @return
     */
    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId, String stockKey) {
        // 一人一单逻辑，判断用户是否已经下过单
        Integer count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("无法重复购买~");
        }
        // 减少库存
        stringRedisTemplate.opsForValue().decrement(stockKey);
        // 使用乐观锁防止超卖，除此之外可以新增一个版本号字段，但失败率较高
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 1)
                .eq("voucher_id", voucherId).update();
        if (!success) {
            stringRedisTemplate.opsForValue().increment(stockKey);
            return Result.fail("库存不足~");
        }
        // 6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId(stockKey));
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setCreateTime(LocalDateTime.now());
        voucherOrder.setUserId(userId);
        voucherOrder.setStatus(1);
        save(voucherOrder);

        return Result.ok(voucherOrder.getId());
    }
}
