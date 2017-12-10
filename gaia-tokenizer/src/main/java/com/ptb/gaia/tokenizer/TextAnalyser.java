package com.ptb.gaia.tokenizer;

import java.util.List;

/**
 * Created by eric on 16/7/18.
 */
public interface TextAnalyser {
	List<GToken> getHtmlTokens(String html);

	List<GToken> getTokens(String sentences);

	List<String> getKeyWords(String sentences);

	List<String> getHtmlKeywords(String html);

}
