package com.gjxx.chap04;

import org.springframework.stereotype.Service;

/**
 * @author Sxs
 * @description 用于告知下游数据流系统，该订单已经处理过了的提示服务
 * @date 2019/10/8 13:52
 */
@Service
public class NotificationServiceImpl implements NotificationService {

    @Override
    public void notifyOrderHasBeenProcessed(Order order) {
        System.out.println(order.getId() + " 订单已经处理...");
    }
}
