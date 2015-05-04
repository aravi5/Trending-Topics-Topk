package storm.starter.trident.project.countmin; 

import java.util.Arrays;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;
import storm.trident.operation.builtin.Count;
import storm.starter.trident.project.countmin.state.BloomFilter;
import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
//aravi5: Import packages required to implement top-k
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.countmin.state.TopkQuery;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.filters.StopWordFilter;
import storm.starter.trident.project.filters.Print;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;

/**
 * Topology that subscribes for tweets and stores the frequency of words in tweets,
 * in a count-min sketch. This is then queried to obtain top-k most frequent words.
 * Originally written by Preetham MS (pmahish@ncsu.edu) and modified by author:
 *@author: Abhishek Ravi(aravi5@ncsu.edu)
 */


public class CountMinSketchTopology {
	

	 public static StormTopology buildTopology( LocalDRPC drpc, String[] args ) {

        TridentTopology topology = new TridentTopology();
        
        int width = 100000;
	    int depth = 50;
	    int seed = 10;
	    
	    //aravi5: Used for only testing purposes.
    	/* 
    	 * FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
			new Values("the cow jumped over the man"),
			new Values("the man went to the store and bought some apples"),
			new Values("four score and seven years ago man ate cow and apples"),
			new Values("how many apples can cow eat"),
			new Values("to be or not to be cow man"))
			;
		spoutFixedBatch.setCycle(true); 
		*/
	    
	    //aravi5: Create a spout that streams tweets from twitter. Use TwitterSampleSpout.
	    //		  Parse the required parameters from the args passed.
	    
		//Twitter's account credentials passed as args
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        //Twitter topic of interest
        String[] arguments = args.clone();
        String[] topicWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        
        //Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, topicWords);

        //aravi5: Used for only testing purposes. Uses spoutFixedBatch.
		
		/* 
		TridentState countMinDBMS = topology.newStream("tweets", spoutFixedBatch)
			.each(new Fields("sentence"), new Split(), new Fields("words"))
			//.each(new Fields("words"), new Print("words field"))
			.each(new Fields("words"), new StopWordFilter())
			//.each(new Fields("words"), new Print("words field"))
			.partitionPersist( new CountMinSketchStateFactory(depth,width,seed), new Fields("words"), new CountMinSketchUpdater())
		;
		*/
		
		/** aravi5:
		 * "tweets" are obtianed from spoutTweets.
		 * Each "tweet" is parsed to get text, tweetId and user.
		 * Obtain only the text from SentenceBuilder.
		 * Split the text by "space" to obtain words.
		 * Filter the stop words using BloomFilter.
		 * Store the frequency of the words in a Count-min Sketch.
		 **/
		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			.parallelismHint(1)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
			//.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			.each(new Fields("sentence"), new Split(), new Fields("words"))
			.each(new Fields("words"), new StopWordFilter())
			//.each(new Fields("words"), new Print("tweetwords"))
			.partitionPersist( new CountMinSketchStateFactory(depth,width,seed), new Fields("words"), new CountMinSketchUpdater())
		;

		/** aravi5: 
		 * Uncomment to query the count of certain words. 
		 **/
		/*
		topology.newDRPCStream("get_count", drpc)
			.each( new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("query", "count"))
		;
		*/
		
		/**
		 * aravi5: Create a new DRPC stream for the request get_topk
		 * Query for the Top-k most frequent items so far.
		 * Split the words based on space.
		 * Query for the frequency of the top words
		 * Project the values returned by the query.
		 **/
		topology.newDRPCStream("get_topk", drpc)
			.stateQuery(countMinDBMS, new Fields("args"), new TopkQuery(), new Fields("topkwords"))
			.each( new Fields("topkwords"), new Split(), new Fields("words"))
			.stateQuery(countMinDBMS, new Fields("words"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("words", "count"))
			//.project(new Fields("words"))
		;
		
		return topology.build();

	}


	public static void main(String[] args) throws Exception {
		
		Config conf = new Config();
        conf.setDebug( false );
        conf.setMaxSpoutPending( 10 );


        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("get_count",conf,buildTopology(drpc, args));

         /** aravi5: 
          * Uncomment to query the count of certain words.
          * Ideally the count of 'a' and 'the' should be 0, 
          * but there could be false positives.
          **/
        /*
         for (int i = 0; i < 5; i++) {
           System.out.println("DRPC RESULT:"+ drpc.execute("get_count","love man a the"));
           Thread.sleep( 5000 );
        } 
        */
        
        //aravi5: Run the loop infinitely and query for top-k frequent items, every 3 seconds.
        for (; ;) {

        	System.out.println("DRPC RESULT:"+ drpc.execute("get_topk","trending"));
        	Thread.sleep( 3000 );
        }
        	
        	

		//System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
