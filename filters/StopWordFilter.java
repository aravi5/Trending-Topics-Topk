package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

//To read the stopwords into BloomFilter
import storm.starter.trident.project.countmin.state.BloomFilter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * BloomFilter that contains a list of stop-words. 
 * All words in the stream are checked against the bloom filter,
 * Stop words are filtered out.
 * 
 * Uses a very well known implementation of Bloom Filter 
 * by - https://github.com/magnuss/java-bloomfilter
 * 
 * @author Abhishek Ravi (aravi5@ncsu.edu)
 */

public class StopWordFilter extends BaseFilter {
	
	private static final String DATA_PATH = "data/stopwords.txt";
	BloomFilter<String> bloomFilter;

	/* public StopWordFilter(String prefix) {
		this.prefix=prefix;
	} */

	public StopWordFilter() {
		
        double falsePositiveProbability = 0.005;
        int expectedNumberOfElements = 200;
        bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
		
		try {
			
			File file1 = new File(DATA_PATH);
			BufferedReader br = new BufferedReader(new FileReader(file1));
					
			String line = br.readLine();
			while(line != null) {
				bloomFilter.add(line);
				line = br.readLine();
			}
			
			br.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		
		//boolean b = (!bloomFilter.contains(tuple.getString(0)));
		//System.out.println("StopWordFilter!!! - " + tuple.getString(0) + " ---> Boolean: " + b);
		//return b;
		return (!bloomFilter.contains(tuple.getString(0)));
	}
}
