package com.qqdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.qqdp.dto.Result;
import com.qqdp.entity.SeckillVoucher;
import com.qqdp.entity.VoucherOrder;
import com.qqdp.mapper.VoucherOrderMapper;
import com.qqdp.service.ISeckillVoucherService;
import com.qqdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.qqdp.utils.*;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;

    // 获取当前类的代理对象，防止事务失效
    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        SeckillVoucher voucher = cacheClient.queryWithPassThrough(
                RedisConstants.CACHE_SECKILL_VOUCHER_KEY, voucherId, SeckillVoucher.class,
                id -> {
                    SeckillVoucher result = seckillVoucherService.getById(id);
                    if (result != null
                            && StrUtil.isNotBlank(stringRedisTemplate.opsForValue().get(stockKey))) {
                        // 数据预热，设置库存缓存
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

        // 同步下单
//        return synchronous(voucherId);
        // 异步下单
        return asynchronous(voucherId);
    }

    // lua 脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 加载 lua 脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // ClassPathResource 可以获取 class 路径下的资源
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 异步执行数据下单的线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    // 存储下单信息的阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 执行循环下单的任务类
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                VoucherOrder order;

                // 从阻塞队列中获取下单信息并操作下单
//                try {
//                    order = orderTasks.take();
//                    // 2.创建订单
//                    proxy.createVoucherOrder(order);
//                } catch (Exception e) {
//                    log.error("订单处理异常", e);
//                }

                // 从 redis 消息队列中获取下单信息并操作下单
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            // 创建一个消费者
                            Consumer.from("g1", "c1"),
                            // 获取一条数据，若队列为空，阻塞两秒，防止 cpu 空转
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            // 从 stream.orders 消息队列中获取最新的一条数据
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    order = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(order);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理异常消息
                    // 从消息队列中获取消息后，消息会存放在 pending-list 中，
                    // 直到消息得到确认才从中删除，若出现异常需要在 pending-list 重新获取消息进行处理
                    handlePendingList();
                }
            }
        }

        // 处理 pending-list 中的消息
        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            // 创建一个消费者
                            Consumer.from("g1", "c1"),
                            // 获取一条数据
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理 pendding 订单异常", e);
                    try{
                        Thread.sleep(20);
                    }catch(Exception ex){
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 创建订单
     *
     * @param order
     */
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder order) {
        Long voucherId = order.getVoucherId();
        // 使用乐观锁防止超卖，除此之外还可以新增一个版本号字段，但失败率较高
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 1)
                .eq("voucher_id", voucherId).update();
        if (!success) {
            log.error("库存不足，下单失败");
        }
        // 创建订单
        save(order);
    }

    /**
     * 异步下单，将校验和具体下单分开执行，在 redis 中进行资格校验，
     * 成功后将数据存入阻塞队列，异步执行具体下单的步骤
     *
     * @param voucherId
     * @return
     */
    private Result asynchronous(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单号
        long orderId = redisIdWorker.nextId("order");
        // 执行 lua 脚本进行库存和一人一单校验
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        // 2.判断结果是否为0
        if (result > 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(result == 1 ? "库存不足~" : "无法重复购买~");
        }
        // 获得原始的事务对象
        if (proxy == null) {
            proxy = (IVoucherOrderService) AopContext.currentProxy();
        }
        // 将订单信息保存到阻塞队列，异步执行数据库下单操作
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setCreateTime(LocalDateTime.now());
        voucherOrder.setUserId(userId);
        voucherOrder.setStatus(1);
        orderTasks.add(voucherOrder);
        // 3.返回订单id
        return Result.ok(orderId);
    }

    /**
     * 同步下单，校验完成后直接进行真正的下单，期间会操作多次数据库，效率低下
     *
     * @param voucherId
     * @return
     */
    private Result synchronous(Long voucherId) {
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        // 判断库存是否充足
        String stock = stringRedisTemplate.opsForValue().get(stockKey);
        if (Integer.parseInt(stock) < 1) {
            return Result.fail("库存不足~");
        }
        // 如果锁加在方法内部，因为该方法被 spring 的事务控制，
        // 会导致当前方法事务还没有提交，但是锁已经释放也会导致问题，
        // 所以选择将当前方法整体包裹起来，确保事务不会出现问题
        Long userId = UserHolder.getUser().getId();
        // 使用分布式系统或集群模式下多进程可见并且互斥的锁
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate,
                voucherId + ":" + userId);
        if (simpleRedisLock.tryLock(10)) {
            // 调用的方法，其实是this.的方式调用的，事务想要生效，
            // 还得利用代理来生效，所以这个地方，我们需要获得原始的事务对象，来操作事务
            // return this.createVoucherOrder(voucherId, userId, stockKey);
            if (proxy == null) {
                proxy = (IVoucherOrderService) AopContext.currentProxy();
            }
            return proxy.createVoucherOrder(voucherId, userId, stockKey);
        }
        return Result.fail("业务繁忙~");

        // 单机情况的锁
//        synchronized (voucherId + ":" + userId) {
//            // 调用的方法，其实是this.的方式调用的，事务想要生效，
//            // 还得利用代理来生效，所以这个地方，我们需要获得原始的事务对象，来操作事务
//            // return this.createVoucherOrder(voucherId, userId, stockKey);
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId, userId, stockKey);
//        }
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
        // 使用乐观锁防止超卖，除此之外还可以新增一个版本号字段，但失败率较高
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
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setCreateTime(LocalDateTime.now());
        voucherOrder.setUserId(userId);
        voucherOrder.setStatus(1);
        save(voucherOrder);

        return Result.ok(voucherOrder.getId());
    }
}
