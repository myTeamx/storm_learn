package com.gjxx.chap04;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Sxs
 * @description 调用外部服务验证订单中信用卡的授权信息，
 *              并将验证结果更新至数据库
 * @date 2019/10/8 11:58
 */
@Component
public class AuthorizeCreditCard extends BaseBasicBolt {

    /**
     * 信用卡授权验证服务
     */
    private final AuthorizationService authorizationService;

    /**
     * 操作订单数据库
     */
    private final OrderDao orderDao;

    @Autowired
    public AuthorizeCreditCard(AuthorizationService authorizationService, OrderDao orderDao) {
        this.authorizationService = authorizationService;
        this.orderDao = orderDao;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Order order = (Order) input.getValueByField("order");
        boolean isAuthorized = authorizationService.authorize(order);
        if (isAuthorized) {
            // 更新订单状态为 准备发货
            orderDao.updateStatusToReadyToShip(order);
        } else {
            // 更新订单状态为 验证失败
            orderDao.updateStatusToDenied(order);
        }
        collector.emit(new Values(order));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }

}
