package com.ptb.gaia.etl.flume.common;

import com.ptb.gaia.etl.flume.entity.ArticleBrandStatic;

import java.util.*;

/**
 * Created by MengChen on 2017/3/21.
 */
public class ArticleBrandFilter {

    /**
     * @Description:Brand Filter
     * @param: [brandlist, brandKeyWordsMap, brandCountMap, keywordList]
     * @return: java.lang.Long
     * @author: mengchen
     * @Date: 2017/3/21
     */
    public static List<Long> BrandFilter(List<Long> brandlist, Map<Long, List<String>> brandKeyWordsMap,
                                         Map<Long, Integer> brandCountMap, List<String> keywordList) {

        Map<Long, Integer> brandSortMap = new LinkedHashMap<>();
        brandlist.forEach(x -> {
            List<String> brandSearchList = brandKeyWordsMap.get(x);
            keywordList.forEach(kw -> {
                if (brandSearchList.contains(kw)) {
                    brandCountMap.put(x, brandCountMap.get(x) + 1);
                }
            });
        });
        brandCountMap.entrySet().stream().filter(x -> x.getValue() > 1).sorted(Map.Entry.<Long, Integer>comparingByValue().reversed())
                .forEachOrdered(x -> brandSortMap.put(x.getKey(), x.getValue()));

        if (brandSortMap.isEmpty()) {
            return Collections.singletonList(ArticleBrandStatic.Other);
        }

//        int maxNum = brandSortMap.entrySet().stream().findFirst().get().getValue();
//        if (brandSortMap.isEmpty() || maxNum == 1) {
//            return Collections.singletonList(ArticleBrandStatic.Other);
//        } else if (maxNum >= 2 && maxNum <= 4) {
//            return Collections.singletonList(brandSortMap.entrySet().stream().findFirst().get().getKey());
//        }

        List<Long> brandList = new ArrayList<>();
        brandSortMap.entrySet().forEach(x -> {
            brandList.add(x.getKey());
        });

        return brandList;
    }


}
