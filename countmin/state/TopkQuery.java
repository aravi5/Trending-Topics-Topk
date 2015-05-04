//import CountMinSketchState;
package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import backtype.storm.tuple.Values;


/**
 * To Query the k most frequent items (in terms of count) stored in Priority Queue.
 *@author: Abhishek Ravi (aravi5@ncsu.edu)
 */


public class TopkQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList<String>();
    	String str = "";
    	
    	// Print the elements of the heap using an iterator
        Iterator<String> it = state.heap.iterator();
        
       while(it.hasNext()) {
    	   
    	   String s = it.next();
    	   str += s;
    	   str+= " ";
        }
       ret.add(str);
        return ret;
    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }    
}
