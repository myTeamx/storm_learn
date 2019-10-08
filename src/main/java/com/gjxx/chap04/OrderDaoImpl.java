package com.gjxx.chap04;

import org.springframework.stereotype.Repository;

/**
 * @author Sxs
 * @description 操作数据库订单
 * @date 2019/10/8 12:07
 */
@Repository
public class OrderDaoImpl implements OrderDao {

    /**
     * 更新订单状态为 准备发货
     * @param order 订单
     * @return 更新成功
     */
    @Override
    public int updateStatusToReadyToShip(Order order) {
        return 1;
    }

    /**
     * 更新订单状态为 验证失败
     * @param order 订单
     * @return 更新成功
     */
    @Override
    public int updateStatusToDenied(Order order) {
        return 1;
    }
}
