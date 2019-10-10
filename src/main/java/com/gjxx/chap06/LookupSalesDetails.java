package com.gjxx.chap06;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author Sxs
 * @description 查看推荐商品的详细信息
 * @date 2019/10/9 14:28
 */
public class LookupSalesDetails extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String customerId = input.getStringByField("customerId");
        String saleIds = input.getStringByField("saleIds");
        String sales = null;
        try {
            sales = saleIds + "详细信息";
        } catch (Exception e) {
            // 报错，但是继续执行，不会重试
            outputCollector.reportError(e);
        }
        if (StringUtils.isEmpty(sales)) {
            outputCollector.fail(input);
        } else {
            outputCollector.emit(new Values(customerId, sales));
            outputCollector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("customerId", "sales"));
    }

}
