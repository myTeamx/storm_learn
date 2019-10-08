package com.gjxx.chap04;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author Sxs
 * @description 调用外部服务验证订单中信用卡的授权信息，
 *              并将验证结果更新至数据库
 * @date 2019/10/8 14:21
 */
@Component
public class AuthorizeCreditCard2 extends BaseRichBolt {

    private final AuthorizationService authorizationService;

    private final OrderDao orderDao;

    private OutputCollector outputCollector;

    @Autowired
    public AuthorizeCreditCard2(AuthorizationService authorizationService, OrderDao orderDao) {
        this.authorizationService = authorizationService;
        this.orderDao = orderDao;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Order order = (Order) input.getValueByField("order");
        try {
            boolean isAuthorized = authorizationService.authorize(order);
            if (isAuthorized) {
                orderDao.updateStatusToReadyToShip(order);
            } else {
                orderDao.updateStatusToDenied(order);
            }
            // 锚定输入元组
            this.outputCollector.emit(input, new Values(order));
            // 应答输入元组
            this.outputCollector.ack(input);
        } catch (Exception e) {
            // 当抛出异常时为输入元组报错
            this.outputCollector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }

}
