package com.ptb.gaia.tokenizer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.lionsoul.jcseg.extractor.impl.TextRankKeywordsExtractor;
import org.lionsoul.jcseg.tokenizer.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by eric on 16/7/18.
 */
public class JSegBrandTextAnalyser implements TextAnalyser {
    static Logger logger = LoggerFactory.getLogger(JSegBrandTextAnalyser.class);
    private static TextAnalyser textAnalyser;
    static Integer lock = new Integer(1);

    static public TextAnalyser i() throws ConfigurationException {
        if (textAnalyser == null) {
            synchronized (lock) {
                if (textAnalyser == null) {
                    textAnalyser = new JSegBrandTextAnalyser();
                }
            }
        }
        return textAnalyser;
    }

    ADictionary dictionary;
    JcsegTaskConfig config;

    static JcsegTaskConfig MediaCategoryConfig() {
        JcsegTaskConfig config = new JcsegTaskConfig(true);
        PropertiesConfiguration conf = null;
        try {
            conf = new PropertiesConfiguration("ptb.properties");
            config = new JcsegTaskConfig(true);
            config.setClearStopwords(conf.getBoolean("gaia.tokenizer.jseg.clear.stopword", true));
            config.setAppendCJKSyn(conf.getBoolean("gaia.tokenizer.jseg.cjksyn", false));
            config.setKeepUnregWords(conf.getBoolean("gaia.tokenizer.jseg.unregword", false));
            config.setMaxLength(conf.getInt("gaia.tokenizer.jseg.maxlength", 10));
            config.setSTokenMinLen(conf.getInt("gaia.tokenizer.jseg.minlength", 2));
            config.setICnName(true);
            config.setMaxCnLnadron(5);
            config.setCnNumToArabic(false);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return config;
    }


    private JSegBrandTextAnalyser() throws ConfigurationException {
        config = MediaCategoryConfig();
        dictionary = DictionaryFactory.createDefaultDictionary(config, true);
        try {
            dictionary.load(this.getClass().getResourceAsStream("/lexicon/lex-brand.lex"));
        } catch (IOException e) {
            System.out.println("自定义词库目录不存在,将使用默认词库");
        }
    }

    @Override
    public List<GToken> getHtmlTokens(String html) {
        Document document = Jsoup.parseBodyFragment(html);
        String text = document.text();
        return getTokens(text);
    }

    @Override
    public List<GToken> getTokens(String sentences) {
        List<GToken> tokens = new ArrayList<>();
        try {
            ISegment jcseg = SegmentFactory.createJcseg(JcsegTaskConfig.DETECT_MODE, new StringReader(sentences), config, dictionary);
            IWord word;
            while ((word = jcseg.next()) != null) {
                tokens.add(new GToken(word.getValue(), word.getFrequency()));
            }
        } catch (JcsegException e) {
            logger.error("分词失败,cause", e);
        } catch (IOException e) {
            logger.error("分词失败,cause", e);
        }
        return tokens;
    }

    @Override
    public List<String> getKeyWords(String sentences) {
        try {
            ISegment jcseg = SegmentFactory.createJcseg(JcsegTaskConfig.COMPLEX_MODE, new StringReader(sentences), config, dictionary);
            TextRankKeywordsExtractor extractor5 = new TextRankKeywordsExtractor(jcseg);
            extractor5.setKeywordsNum(30);
            extractor5.setMaxIterateNum(100);
            extractor5.setAutoFilter(false);
            return extractor5.getKeywordsFromString(sentences);
        } catch (JcsegException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<String> getHtmlKeywords(String html) {
        return getKeyWords(Jsoup.parseBodyFragment(html).text());

    }

    public static void main(String[] args) throws ConfigurationException {
//        List<String> tokens = JSegBrandTextAnalyser.i().getKeyWords("【雪铁龙C3-XR】4.98万任性开回家,东风雪铁龙首款SUV C3-XR，定位于目前SUV市场中增长最快的紧凑型SUV细分市场。C3-XR以“自由我精彩”的鲜活态度，从动力、操控、造型等方面为城市SUV用户带来全新价值的愉悦体验，彰显“城市SUV新风范”。 为回馈广大客户，在东风雪铁龙25周年厂庆期间，本店特推出2017款C3-XR 1.2THP手动先锋型 特惠领航版，现只需4.98万即可任性开回家。 优惠详情—— 提前“享”，轻松“购”。 原价：13.48万元 心动价：11.98万元 首付：4.98万元 日供：98元 首付只需4.98万元，日供只需98元，你还在等什么呢！！！！！！！！ 这个价格，除了省钱，你还能想到什么~~~~~ ↓↓↓↓↓↓↓↓↓↓点击下方阅读原文了解更多本店特惠信息，总有一款适合你！");
//        for (String token : tokens) {
//            System.out.println(token);
//
//        }

//        JSegBrandTextAnalyser.i().getTokens("福特（Ford）是世界著名的汽车品牌，为美国福特汽车公司（Ford Motor Company）旗下的众多品牌之一，公司及品牌名“福特”来源于创始人亨利·福特（Henry Ford）的姓氏。福特汽车公司是世界上最大的汽车生产商之一，成立于1903年，旗下拥有福特（Ford）和林肯（Lincoln）汽车品牌，总部位于密歇根州 迪尔伯恩市（Dearborn）。除了制造汽车，公司还设有金融服务部门即福特信贷（Ford Credit），主要经营购车金融、车辆租赁和汽车保险方面的业务。")
//                .forEach(x -> {
//                    System.out.println(x.getWord());
//                });
                }
}
