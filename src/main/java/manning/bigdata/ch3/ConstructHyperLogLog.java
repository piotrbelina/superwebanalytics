package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 15:01
 * To change this template use File | Settings | File Templates.
 */
public class ConstructHyperLogLog extends CascalogBuffer {
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        HyperLogLog hll = new HyperLogLog(8192);
        Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
        while (it.hasNext()) {
            TupleEntry tuple = it.next();
            hll.offer(tuple.getObject(0));
        }

        try {
            bufferCall.getOutputCollector().add(new Tuple(hll.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
