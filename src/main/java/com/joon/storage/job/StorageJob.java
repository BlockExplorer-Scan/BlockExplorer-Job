package com.joon.storage.job;

import com.joon.storage.utils.CommonUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;
import org.web3j.protocol.http.HttpService;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * @program storage-job
 * @author: joon.h
 * @create: 2019/02/20 14:50
 */
@Component
@EnableScheduling
public class StorageJob {

    Logger logger = LoggerFactory.getLogger(StorageJob.class);

    @Autowired
    TransportClient client;
    @Autowired
    Web3j web3j;

    @Scheduled(fixedRate = 1000 * 10000000)
    public void countTokenAccount() {
        logger.info(" joon -- StorageJob - erc20  - start ");
        try {
            /**
             * 统计出共有多少代币
             */
            BoolQueryBuilder boolQueryBuilderToken = new BoolQueryBuilder();
            boolQueryBuilderToken.must(new TermQueryBuilder("status", "erc20"));
            //聚合处理
            SearchSourceBuilder sourceBuilderToken = new SearchSourceBuilder();
            TermsAggregationBuilder termsAggregationBuilderToken = AggregationBuilders.terms("group_token_count").field("address");
            sourceBuilderToken.aggregation(termsAggregationBuilderToken);
            sourceBuilderToken.query(boolQueryBuilderToken);
            //查询索引对象
            SearchRequest searchRequestToken = new SearchRequest("erc20");
            searchRequestToken.types("data");
            searchRequestToken.source(sourceBuilderToken);
            SearchResponse responseToken = client.search(searchRequestToken).get();
            Terms termsToken = responseToken.getAggregations().get("group_token_count");
            logger.info(" joon -- StorageJob - erc20 代币量 -- {}", termsToken.getBuckets().size());
            //遍历所有代币
            for (Terms.Bucket token : termsToken.getBuckets()) {
                /**
                 * 获取代币下有多少账户
                 */
                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                boolQueryBuilder.must(new TermQueryBuilder("status", "erc20"));
                boolQueryBuilder.must(new TermQueryBuilder("address", token.getKey()));
                //聚合处理
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group_to_count").field("to");
                sourceBuilder.aggregation(termsAggregationBuilder);
                sourceBuilder.query(boolQueryBuilder);

                //查询索引对象
                SearchRequest searchRequest = new SearchRequest("erc20");
                searchRequest.types("data");
                searchRequest.source(sourceBuilder);
                SearchResponse response = client.search(searchRequest).get();

                Terms terms = response.getAggregations().get("group_to_count");
                logger.info(" joon -- StorageJob - erc20 代币 : {} - 账户地址数量 : {}", token.getKey(), terms.getBuckets().size());
                //遍历代币内所有账户
                for (Terms.Bucket entry : terms.getBuckets()) {
                    /**
                     * 获取账户余额 并 存入索引表中
                     */
                    BigInteger balance = getTokenBalance(web3j, entry.getKey().toString(), token.getKey().toString());
                    logger.info(" joon -- StorageJob - erc20 代币 : {} - 账户地址数量 : {} - 账户地址 : {} - 账户余额 : {}",
                            token.getKey(), terms.getBuckets().size(), entry.getKey().toString(), balance);

                    //处理balance
                    BigDecimal percentage = getPercentage(token.getKey().toString(), balance);

                    /**
                     * 以下是插入到新的索引表中 （待处理）
                     */


                    try (
                            XContentBuilder content = XContentFactory.jsonBuilder().startObject()
                                    .field("erc20name", token.getKey())
                                    .field("address", entry.getKey().toString())
                                    .field("addressNumber", terms.getBuckets().size())
                                    .field("quantity", balance.doubleValue())
                                    .field("percentage", percentage.doubleValue())
                                    .endObject())
                    {
                        IndexResponse response2 = client.prepareIndex("erc20token","data")
                                .setSource(content)
                                .get();

                        client.prepareUpdate();
                        logger.info("[ERC20Token统计信息]存入ES成功...");



                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }

            logger.info(" joon -- StorageJob - erc20  - end ");

            //开始删除前一天数据

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 查询代币余额
     */
    public BigInteger getTokenBalance(Web3j web3j, String fromAddress, String contractAddress) {

        BigInteger balanceValue = BigInteger.ZERO;

        try {
            Function function = new Function("balanceOf",
                    Arrays.<Type>asList(new Address(fromAddress)),
                    Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {
                    }));
            String data = FunctionEncoder.encode(function);
            Transaction transaction = Transaction.createEthCallTransaction(fromAddress, contractAddress, data);

            EthCall ethCall = web3j.ethCall(transaction, DefaultBlockParameterName.LATEST).send();
            List<Type> results = FunctionReturnDecoder.decode(ethCall.getValue(), function.getOutputParameters());
            balanceValue = (BigInteger) results.get(0).getValue();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return balanceValue;
    }

    /**
     * 统计百分比
     */

    public BigDecimal getPercentage (String contractAddress, BigInteger tokenBalance){
//        Web3j web3 = Web3j.build(new HttpService("http://n8.ledx.xyz"));

        BigInteger tokenTotalSupply = CommonUtils.getTokenTotalSupply(web3j, contractAddress);
        BigDecimal tokenTotalSupply1 =new BigDecimal(tokenTotalSupply);

        BigDecimal tokenBalance1 =new BigDecimal(tokenBalance);
        if (tokenTotalSupply1.toString() == "0") {
            return BigDecimal.ZERO;
        } else {
            BigDecimal divide = tokenBalance1.divide(tokenTotalSupply1, 6, BigDecimal.ROUND_HALF_UP);
            return divide;
        }



    }
}
