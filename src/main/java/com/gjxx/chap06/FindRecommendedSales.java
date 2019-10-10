package com.gjxx.chap06;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

/**
 * @author Sxs
 * @description 查找i推荐的商品
 * @date 2019/10/9 11:54
 */
public class FindRecommendedSales extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("customerId", "saleIds"));
    }

    /**
     * 执行完该方法，对应元组，自动应答
     * @param input 输入元组
     * @param collector 发射器
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String customerId = (String) input.getValueByField("customerId");
        try {
            String saleIds = customerId + "的商品列表";
            if (StringUtils.isNotEmpty(saleIds)) {
                // 发射后，元组自动锚定
                collector.emit(new Values(customerId, saleIds));
            }
        } catch (Exception e) {
            // 可以在storm ui上看到这里抛出的异常
            // 相当于提示storm，该元组fail，需要重试
            throw new ReportedFailedException(e);
        }
    }
}
