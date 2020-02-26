package com.joon.storage.utils;

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

import java.io.IOException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @program BlockExplorer-Web-h5
 * @create: 2019/01/21 13:31
 */

public class CommonUtils {


    public static boolean isNumeric0(String str) {
        for (int i = str.length(); --i >= 0; ) {
            int chr = str.charAt(i);
            if (chr < 48 || chr > 57)
                return false;
        }
        return true;
    }


    /**
     * @date 2019-9-10
     * @param web3j
     * @param contractAddress
     * @return
     */
    public static BigInteger getTokenTotalSupply(Web3j web3j, String contractAddress) {

        BigInteger totalSupply = BigInteger.ZERO;
        try {
            Function function = new Function("totalSupply",
                    Arrays.<Type>asList(),
                    Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {
                    }));

            String data = FunctionEncoder.encode(function);
            org.web3j.protocol.core.methods.request.Transaction transaction =
                    Transaction.createEthCallTransaction(contractAddress, contractAddress, data);

            EthCall ethCall = web3j.ethCall(transaction, DefaultBlockParameterName.LATEST).sendAsync().get();
            List<Type> results = FunctionReturnDecoder.decode(ethCall.getValue(), function.getOutputParameters());

            if (results.size() > 0) {
                totalSupply = (BigInteger) results.get(0).getValue();

                return totalSupply;
            }
            return BigInteger.ZERO;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return totalSupply;
    }


    /**
     * 查询代币精度
     *
     * @param web3j
     * @param contractAddress
     * @return
     */
    public static int getTokenDecimals(Web3j web3j, String contractAddress) {

        int decimal = 0;

        try {

            Function function = new Function("decimals",
                    Arrays.<Type>asList(),
                    Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {
                    }));

            String data = FunctionEncoder.encode(function);
            Transaction transaction = Transaction.createEthCallTransaction(contractAddress, contractAddress, data);
            EthCall ethCall = web3j.ethCall(transaction, DefaultBlockParameterName.LATEST).sendAsync().get();
            List<Type> results = FunctionReturnDecoder.decode(ethCall.getValue(), function.getOutputParameters());
            decimal = Integer.parseInt(results.get(0).getValue().toString());

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return decimal;
    }

    public static String getCurrentTime(){

        long currentTimeMillis = System.currentTimeMillis();
        String format = new SimpleDateFormat("yyyy-MM-dd").format(currentTimeMillis);
        return format;
    }

    public static String getLastTime(){

        long l = System.currentTimeMillis() - 86400000;
        String format = new SimpleDateFormat("yyyy-MM-dd").format(l);
        return format;
    }


}
