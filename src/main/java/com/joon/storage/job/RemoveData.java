package com.joon.storage.job;

import com.joon.storage.utils.CommonUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;

/**
 * @author soobeenwong
 * @date 2019-02-22 5:09 PM
 * @desc 定时任务：删除前一天统计的Holds数据
 */
@Component
@EnableScheduling
@RestController
@RequestMapping("/RemoveData")
public class RemoveData {

    @Autowired
    private TransportClient client;

    @PostMapping("/delete")
    public ResponseEntity delete(@RequestParam(name = "id", defaultValue = "false") String id){

        String currentTime = CommonUtils.getCurrentTime();
        String lastTime = CommonUtils.getLastTime();

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                // 根据条件个数添加filter语句
                .filter(QueryBuilders.matchQuery("time", lastTime))
                .source("erc20token")
                .get();
        long deleted = response.getDeleted();




        return new ResponseEntity(deleted, HttpStatus.OK);

    }



}
