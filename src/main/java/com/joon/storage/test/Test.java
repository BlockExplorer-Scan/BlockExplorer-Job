package com.joon.storage.test;

import com.joon.storage.utils.CommonUtils;
import org.elasticsearch.action.delete.DeleteResponse;

import java.text.SimpleDateFormat;

/**
 * @author soobeenwong
 * @date 2019-03-04 5:53 PM
 * @desc 测试类
 */
public class Test {

    public static void main(String[] args) {

        try{
            int a = 5/0;
            System.out.println(a);
        }catch (Exception e) {
            return;
        }
        System.out.println("123123123");

    }

}
