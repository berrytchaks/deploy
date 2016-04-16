package testflink.deploy;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {
	
	  //Collector used to emit output
	  SpoutOutputCollector _collector;
	  //Used to generate a random number
	  Random _rand;

	  //Open is called when an instance of the class is created
	  
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		  
	  //Set the instance collector to the one passed in
	    _collector = collector;
	    //For randomness
	    _rand = new Random();
	  }

	  //Emit data to the stream
	  public void nextTuple() {
	  //Sleep for a bit
	    Utils.sleep(100);
	    //The sentences that will be randomly emitted
	    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
	        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
	    //Randomly pick a sentence
	    String sentence = sentences[_rand.nextInt(sentences.length)];
	    //Emit the sentence
	    _collector.emit(new Values(sentence));
	  }

	  //Ack is not implemented since this is a basic example
	  @Override
	  public void ack(Object id) {

	  }

	  //Fail is not implemented since this is a basic example
	  @Override
	  public void fail(Object id) {
	  }

	  //Declare the output fields. In this case, an sentence
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("sentence"));
	  }

	}
