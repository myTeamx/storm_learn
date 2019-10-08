package com.gjxx.chap04;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Sxs
 * @description 通知外部系统，这个订单已经处理过了
 * @date 2019/10/8 13:49
 */
@Component
public class ProcessedOrderNotification extends BaseBasicBolt {

    /**
     * 用于告知下游数据流系统，该订单已经处理过了的，提示服务
     */
    private final NotificationService notificationService;

    @Autowired
    public ProcessedOrderNotification(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Order order = (Order) input.getValueByField("order");
        // 提示下游数据流系统，该订单已经处理过了
        notificationService.notifyOrderHasBeenProcessed(order);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
