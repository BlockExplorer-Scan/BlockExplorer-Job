package com.joon.storage.test;

import com.joon.storage.utils.CommonUtils;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author soobeenwong
 * @date 2019-03-04 5:53 PM
 * @desc 测试类
 */
public class Test {

    public static void main(String[] args) {

        Web3j web3 = Web3j.build(new HttpService("http://n10.ledx.xyz"));
        BigInteger tokenTotalSupply = CommonUtils.getTokenTotalSupply(web3, "0x6c8d4a931712ddf001b29a9585665aaff68211da");
        BigDecimal tokenTotalSupply1 =new BigDecimal(tokenTotalSupply);

        BigDecimal tokenBalance1 =new BigDecimal("0");
        if (tokenTotalSupply1.equals("0")) {
            System.out.println("==" + BigDecimal.ZERO);
        } else {
            BigDecimal divide = tokenBalance1.divide(tokenTotalSupply1, 6, BigDecimal.ROUND_HALF_UP);
            System.out.println(divide);
        }



    }

}
