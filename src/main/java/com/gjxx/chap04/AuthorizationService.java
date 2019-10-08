package com.gjxx.chap04;

/**
 * @author Sxs
 * @description 信用卡授权验证服务
 * @date 2019/10/8 12:01
 */
public interface AuthorizationService {

    boolean authorize(Order order);

}
