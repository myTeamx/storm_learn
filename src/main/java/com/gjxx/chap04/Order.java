package com.gjxx.chap04;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Sxs
 * @description 订单实体
 * @date 2019/10/8 13:36
 */
@Data
public class Order implements Serializable {

    private long id;

    private long customerId;

    private long creditCardNumber;

    private String creditCardExpiration;

    private int creditCardCode;

    private double chargeAmount;

}
