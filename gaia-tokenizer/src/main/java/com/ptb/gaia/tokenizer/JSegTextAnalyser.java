package com.ptb.gaia.tokenizer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.lionsoul.jcseg.extractor.impl.TextRankKeywordsExtractor;
import org.lionsoul.jcseg.tokenizer.core.ADictionary;
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory;
import org.lionsoul.jcseg.tokenizer.core.ISegment;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.lionsoul.jcseg.tokenizer.core.JcsegException;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by eric on 16/7/18.
 */
public class JSegTextAnalyser implements TextAnalyser {
    static Logger logger = LoggerFactory.getLogger(JSegTextAnalyser.class);
    private static TextAnalyser textAnalyser;
    static Integer lock = new Integer(1);

    static public TextAnalyser i() throws ConfigurationException {
        if (textAnalyser == null) {
            synchronized (lock) {
                if (textAnalyser == null) {
                    textAnalyser = new JSegTextAnalyser();
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
            config.setMaxCnLnadron(4);
            config.setCnNumToArabic(false);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return config;
    }


    private JSegTextAnalyser() throws ConfigurationException {
        config = MediaCategoryConfig();
        dictionary = DictionaryFactory.createDefaultDictionary(config, true);
        try {
            dictionary.load(this.getClass().getResourceAsStream("/lexicon/lex-ptb.lex"));
            dictionary.load(this.getClass().getResourceAsStream("/lexicon/lex-stop.lex"));
//			dictionary.load(this.getClass().getResourceAsStream("/lexicon/lex-stop.lex"));
//			dictionary.loadDirectory(getClass().getResource("/lexicon/").getPath());
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
            extractor5.setKeywordsNum(50);
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
//        long start = System.currentTimeMillis();
//        int count = 500000;
//        int i = 0;
///*		while(i++< count) {*/
//        List<String> tokens = JSegTextAnalyser.i().getKeyWords("冷笑话");
//        for (String token : tokens) {
//            System.out.println(token);
//
//        }
        //List<String> aa = JSegTextAnalyser.i().getHtmlKeywords("<h1>你真混球</h1>");
        /*	for (Object s : aa1) {
				System.out.println(s);
			}*/
/*		}*/
/*		System.out.println(count/((System.currentTimeMillis() - start)/1000));*/
    }
}
