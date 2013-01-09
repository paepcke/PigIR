package edu.stanford.pigir.pigudf;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class NGramUtil {

	static boolean DO_ALL_NGRAMS  = true;
	static boolean NOT_ALL_NGRAMS = false;
	/**
	 * This function splits a search query string into a set 
	 * of non-empty words 
	 */
	protected static String[] splitToWords(String query) {
		List<String> res = new LinkedList<String>();
		String[] words = query.split("\\W");
		for (String word : words) {
			if (!word.equals("")) {
				res.add(word);
			}
		}
		return res.toArray(new String[res.size()]);
	}

	/**
	 *   This is a simple utility function that make word-level
	 * ngrams from a set of words
	 * @param words
	 * @param ngrams
	 * @param size
	 */
	protected static void makeNGram(String[] words, Set<String> ngrams, int ngramSize) {
		NGramUtil.makeNGram(words, ngrams, ngramSize, NGramUtil.NOT_ALL_NGRAMS);
	}
	protected static void makeNGram(String[] words, Set<String> ngrams, int maxN, boolean doAllNgrams) {
		if (maxN == -1)
			maxN = 2;
		int stop = words.length - maxN + 1;
		for (int i = 0; i < stop; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < maxN; j++) {
				sb.append(words[i + j]).append(" ");
			}
			sb.deleteCharAt(sb.length() - 1);
			ngrams.add(sb.toString());
		}

		if (doAllNgrams && maxN > 1) {
			makeNGram(words, ngrams, maxN - 1, doAllNgrams);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String content = "This is a nice juicy test";
		String[] words = NGramUtil.splitToWords(content);
		for (String word : words) {
			System.out.println(word);
		}
		Set<String> ngrams = new HashSet<String>();
		int ngramSize = 2;
		NGramUtil.makeNGram(words, ngrams, ngramSize);
		System.out.println(ngrams);
	}
}
