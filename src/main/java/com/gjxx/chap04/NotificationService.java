package com.gjxx.chap04;

/**
 * @author Sxs
 * @description 用于告知下游数据流系统，该订单已经处理过了的提示服务
 * @date 2019/10/8 13:50
 */
public interface NotificationService {

    void notifyOrderHasBeenProcessed(Order order);

}
