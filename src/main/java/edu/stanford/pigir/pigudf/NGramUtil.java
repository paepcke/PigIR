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
 * 
 * Andreas added a bunch of facilities, including optional removal of duplicates.
 */

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class NGramUtil {

	public static boolean DO_ALL_NGRAMS  = true;
	public static boolean NOT_ALL_NGRAMS = false;
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
	 * @param maxArity: how many ngram arities to produce
	 */
	public static void makeNGram(String[] words, Collection<String> ngrams, int maxArity) {
		NGramUtil.makeNGram(words, ngrams, maxArity, NGramUtil.NOT_ALL_NGRAMS);
	}
	
	/**
	 * Heavy lifting for ngram computation. Note: If ngrams, the destination of the results,
	 * is an ArrayList<String>, all ngrams will be available to the caller. If ngrams is 
	 * instead a Set<String>, then duplicates are removed.
	 * @param words array of the words in order
	 * @param ngrams place to put the result, a set
	 * @param maxArity the n of ngrams
	 * @param doAllNgrams if true, computes ngrams from 1 to n. If false, only computes the ngrams for the given n
	 */
	public static void makeNGram(String[] words, Collection<String> ngrams, int maxArity, boolean doAllNgrams) {
		if (maxArity == -1)
			maxArity = 2;
		int stop = words.length - maxArity + 1;
		for (int i = 0; i < stop; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < maxArity; j++) {
				//sb.append(words[i + j]).append(" ");
				sb.append(words[i + j]).append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			ngrams.add(sb.toString());
		}

		if (doAllNgrams && maxArity > 1) {
			makeNGram(words, ngrams, maxArity - 1, doAllNgrams);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String content = "This is a nice juicy test, a nice test.";
		String[] words = NGramUtil.splitToWords(content);
		System.out.println("------------- The Words -----------");		
		for (String word : words) {
			System.out.println(word);
		}
		Set<String> ngrams = new HashSet<String>();
		int ngramSize = 2;
		
		System.out.println("------------- DO_ALL_NGRAMS  Dups Removed -----------");		
		NGramUtil.makeNGram(words, ngrams, ngramSize, DO_ALL_NGRAMS);
		System.out.println(ngrams);
		
		System.out.println("------------- NOT_ALL_NGRAMS Dups Removed -----------");
		ngrams = new HashSet<String>();
		ngramSize = 2;
		NGramUtil.makeNGram(words, ngrams, ngramSize, NOT_ALL_NGRAMS);
		System.out.println(ngrams);
		
		System.out.println("------------- NOT_ALL_NGRAMS, Dups Not Removed -----------");
		ngramsNotSet = new ArrayList<String>();
		ngramSize = 2;
		NGramUtil.makeNGram(words, ngrams, ngramSize, NOT_ALL_NGRAMS);
		System.out.println(ngrams);

		System.out.println("------------- Trigrams  Dups Removed -----------");		
		ngramsNotSet = new ArrayList<String>();
		ngramSize = 3;
		NGramUtil.makeNGram(words, ngrams, ngramSize, NOT_ALL_NGRAMS);
		System.out.println(ngrams);

	}
}
