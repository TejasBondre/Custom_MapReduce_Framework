import java.util.Iterator;


public interface IMapReduce {}

interface IMapper<K1,V1,K2,V2> {
	void map(K1 k, V1 v, OutputCollector o);
}

interface IReducer<K1,V1,K2,V2> {
	void reduce(K1 k, Iterator<V1> values, OutputCollector o);
}
