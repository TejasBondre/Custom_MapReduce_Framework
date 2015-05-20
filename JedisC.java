import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import redis.clients.jedis.Jedis;


public class JedisC implements DataStore {
	List<Jedis> jedisClientList;
	private int redisPort = 6379;
	private int serverId;

	public JedisC(int id) {
		jedisClientList = new ArrayList<Jedis>();

		jedisClientList.add(new Jedis("10.244.35.82",redisPort));
		jedisClientList.add(new Jedis("10.244.35.98",redisPort));
		jedisClientList.add(new Jedis("10.244.35.111",redisPort));
		jedisClientList.add(new Jedis("10.244.35.126",redisPort));
		jedisClientList.add(new Jedis("10.244.35.140",redisPort));

		serverId = id;
	}

	private int getSlot(String key) {
		char c = key.charAt(0);
		int index = c - 'a';
		int slot = (int) (index / 5);
		if(slot != 5) return slot;
		else return 4;
	}

	@Override
	public void write(String key, String value) {
		jedisClientList.get(getSlot(key)).lpush("list", key + ":" + value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> read() {

		List<String> t = jedisClientList.get(serverId).lrange("list", 0, -1);
		return t;
	}

	@Override
	public void writeToMaster(String k, String v) {
		jedisClientList.get(0).lpush("output", k + ":" + v);

	}

/*
 * ./redis-trib.rb create --replicas 0 10.244.35.82:7000 10.244.35.98:7001 10.244.35.111:7002 10.244.35.126:7003 10.244.35.140:7004
 */


}
