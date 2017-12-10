package com.ptb.gaia.tokenizer;

/**
 * Created by eric on 16/7/18.
 */
public class GToken {
	String word;
	int freq;

	public GToken(String value, int frequency) {
		this.word = value;
		this.freq = frequency;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}

	@Override
	public String toString() {
		return "GToken{" +
				"word='" + word + '\'' +
				", freq=" + freq +
				'}';
	}
}
