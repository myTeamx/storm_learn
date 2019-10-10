package com.gjxx.chap06;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

/**
 * @author Sxs
 * @description 本地运行
 * @date 2019/10/10 16:30
 */
public class LocalTopologyRunner {

    private static final int TEN_MINUTES = 1000;

    public static void main(String[] args) {
        Config config = new Config();

        StormTopology topology = FlashSaleTopologyBuilder.build();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("local-flashSale", config, topology);

        // 休眠一段时间，杀掉拓扑，并关闭本地集群
        Utils.sleep(TEN_MINUTES);
        localCluster.killTopology("local-flashSale");
        localCluster.shutdown();

    }

}
