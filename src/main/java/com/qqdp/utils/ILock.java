package com.qqdp.utils;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeSec   锁持有的超时时间
     * @return          获取锁是否成功
     */
    boolean tryLock(long timeSec);

    /**
     * 释放锁
     */
    void unlock();
}
