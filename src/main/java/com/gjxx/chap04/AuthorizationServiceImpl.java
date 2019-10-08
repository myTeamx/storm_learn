package com.gjxx.chap04;

import org.springframework.stereotype.Service;

/**
 * @author Sxs
 * @description 信用卡授权验证服务
 * @date 2019/10/8 12:01
 */
@Service
public class AuthorizationServiceImpl implements AuthorizationService {

    @Override
    public boolean authorize(Order order) {
        return true;
    }
}
