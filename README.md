##Twitter Trending Topics (TopK) in Storm-Trident - Use BloomFilters and CountMinSketch

Academic project to get hands-on experience in building applications on top of Storm-Trident.

A spout that subscribes to Twitter to receive sampled tweets.

Topology that extracts text from Twitter data.

Used probabilistic data structures:
- BloomFilters to remove stop words from Tweet text
- Count-Min Sketch to maintain the count for a particular hashtag

#TODO:
1. Run in distributed mode.

2. Provide a better UI

