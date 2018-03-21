package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class Alibaba {
    /** 请完成下面这个函数，实现题目要求的功能 **/
    /** 当然，你也可以不按照这个模板来作答，完全按照自己的想法来 ^-^  **/
    public static void main(String[] args) {

        List<Integer> order = new ArrayList<Integer>();
        Map<String, List<Integer>> boms = new HashMap<String, List<Integer>>();

        Scanner in = new Scanner(System.in);
        String line = in.nextLine();

        Integer n = Integer.parseInt(line.split(",")[0]);
        Integer m = Integer.parseInt(line.split(",")[1]);

        line = in.nextLine();
        String[] itemCnt = line.split(",");
        for(int i = 0; i < n ; i++){
            order.add(Integer.parseInt(itemCnt[i]));
        }

        for(int i = 0; i < m; i++){
            line = in.nextLine();
            String[] bomInput = line.split(",");
            List<Integer> bomDetail = new ArrayList<Integer>();

            for(int j = 1; j <= n; j++ ){
                bomDetail.add(Integer.parseInt(bomInput[j]));
            }
            boms.put(bomInput[0], bomDetail);
        }
        in.close();

        Map<String, Integer> res = resolve(order, boms);

        System.out.println("match result:");
        for(String key : res.keySet()){
            System.out.println(key+"*"+res.get(key));
        }
    }

    // write your code here
    public static Map<String, Integer> resolve(List<Integer> order, Map<String, List<Integer>> boms) {
    		Map<String,Integer> resultMap=new HashMap<String,Integer>();
    		int goodsNum=order.size();
    		Set<String> bomsKeys= boms.keySet();
    		for(String bomsKey:bomsKeys) {
    			int matchSize=0;
    			int remainTypeNum=0;
    			int remain2=0;
    			List<Integer> bomGoods = boms.get(bomsKey);
    			int min=Integer.MAX_VALUE;
    			for(int i=0;i<goodsNum;i++) {
    				Integer orderIndexNum = order.get(i);
    				Integer bomIndexNum=bomGoods.get(i);
    				if(bomIndexNum>0) {
    					int current=orderIndexNum/bomIndexNum;
    					if(current<min) {
    						min=current;
    					}
    				}
    			}
    			for(int i=0;i<goodsNum;i++) {
    				Integer orderIndexNum = order.get(i);
    				int remain=orderIndexNum%min;
    				if(remain!=0) {
    					remainTypeNum++;
    				}
    			}
    			matchSize=min;
    			resultMap.put(bomsKey, matchSize);
    		}
		return resultMap;
    }
}