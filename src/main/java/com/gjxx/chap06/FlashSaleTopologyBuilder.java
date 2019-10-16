package com.gjxx.chap06;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author Sxs
 * @description 负责连接spout和bolt
 * @date 2019/10/9 11:47
 */
public class FlashSaleTopologyBuilder {

    private static final String CUSTOMER_RETRIEVAL_SPOUT = "customer-retrieval";
    private static final String FIND_RECOMMENDED_SALES = "find-recommended-sales";
    private static final String LOOKUP_SALES_DETAILS = "lookup-sales-details";
    private static final String SAVE_RECOMMENDED_SALES = "save-recommended-sales";

    /**
     * 连接spout和bolt
     * @return TopologyBuilder
     */
    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(CUSTOMER_RETRIEVAL_SPOUT, new CustomerRetrievalSpout())
                // 设置最大等待时间
                .setMaxSpoutPending(250);

        // 设置并行的执行器数量为1，即JVM中对应的线程为1
        builder.setBolt(FIND_RECOMMENDED_SALES, new FindRecommendedSales(), 1)
                // 设置每个执行器中的任务(实例)数为1    (是设置总的task数量还是每个executor中task的数量?)    总的
                .setNumTasks(1)
                .shuffleGrouping(CUSTOMER_RETRIEVAL_SPOUT);

        builder.setBolt(LOOKUP_SALES_DETAILS, new LookupSalesDetails(), 1)
                .setNumTasks(1)
                .shuffleGrouping(FIND_RECOMMENDED_SALES);

        builder.setBolt(SAVE_RECOMMENDED_SALES, new SaveRecommendedSales(), 1)
                .setNumTasks(1)
                .shuffleGrouping(LOOKUP_SALES_DETAILS);

        return builder.createTopology();

    }

}
