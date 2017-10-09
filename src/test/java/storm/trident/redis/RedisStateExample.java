package storm.trident.redis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.InetSocketAddress;

/**
 * 需要外部启动redis
 */
public class RedisStateExample {

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc, StateFactory state) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
            new Values("the cow jumped over the moon"),
            new Values("the man went to the store and bought some candy"),
            new Values("four score and seven years ago"),
            new Values("how many apples can you eat"),
            new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                                          .each(new Fields("sentence"), new Split(), new Fields("word"))
                                          .groupBy(new Fields("word"))
                                          .persistentAggregate(state, new Count(), new Fields("count"))
                                          .parallelismHint(6);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        return topology.build();
    }

    public static void main(String[] args) {
        StateFactory redis = RedisState.nonTransactional(new InetSocketAddress("localhost", 6379));

        LocalDRPC     drpc     = new LocalDRPC();
        StormTopology topology = buildTopology(drpc, redis);
        Config        conf     = new Config();
        LocalCluster  cluster  = new LocalCluster();
        cluster.submitTopology("tester", conf, topology);

        try {
            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC: " + drpc.execute("words", "cat the man four"));
                Utils.sleep(1000);
            }

        } catch (JedisConnectionException e) {
            throw new RuntimeException("Unfortunately, this test requires redis-server runing on localhost:6379", e);
        }

    }
}
