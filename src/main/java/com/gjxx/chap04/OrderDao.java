package com.gjxx.chap04;

/**
 * @author Sxs
 * @description 操作数据库订单
 * @date 2019/10/8 12:07
 */
public interface OrderDao {

    /**
     * 更新订单状态为 准备发货
     * @param order 订单
     * @return 更新成功
     */
    int updateStatusToReadyToShip(Order order);

    /**
     * 更新订单状态为 验证失败
     * @param order 订单
     * @return 更新成功
     */
    int updateStatusToDenied(Order order);

}
