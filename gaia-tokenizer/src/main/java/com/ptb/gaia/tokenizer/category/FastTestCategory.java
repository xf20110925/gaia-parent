package com.ptb.gaia.tokenizer.category;

/**
 * Created by eric on 16/9/9.
 */

public class FastTestCategory {
	public FastTestCategory() {
		System.load("/Users/eric/wk/self/java/fastText/fastText/ljfasttext.so");
		init();
	}
	public FastTestCategory(String libPath) {
		System.load(libPath);
		init();
	}

	private native void init() ;
	private native void deinit();

	public native String[] predicatK(String libPath,String line,int k);

	public static void main(String[] args) {
		FastTestCategory fastTestCategory = new FastTestCategory();
		String a = "claremont high school was a non-denominational state-funded secondary school based in the st leonards area of east kilbride . it closed in 2008 and merged with hunter high school to form the new calderglen high school . it was originally one of 6 state secondary schools in east kilbride until they were merged into 3 schools .";
		int max = 1000000;

		int i = 0;
		long startTime = System.currentTimeMillis();
		while(i++ < max) {
			if(i <= 2) {
				startTime = System.currentTimeMillis();
			}
			String[] predicat = fastTestCategory.predicatK("/Users/eric/wk/self/java/fastText/fastText/result/dbpedia.bin", a, 2);
			for (String s : predicat) {
				System.out.println(s);
			}
		}
	/*	for (String s : predicat) {
			System.out.println(s);
		}*/
		long endTime = System.currentTimeMillis();
		System.out.printf("cost time %d\n",endTime-startTime);
		fastTestCategory.deinit();
	}


}
