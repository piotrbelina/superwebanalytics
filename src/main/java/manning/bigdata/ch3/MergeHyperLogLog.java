package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 15:04
 * To change this template use File | Settings | File Templates.
 */
public class MergeHyperLogLog extends CascalogBuffer{
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
        HyperLogLog merged = null;

        try {
            while (it.hasNext()) {
                TupleEntry tupleEntry = it.next();
                byte[] serialized = (byte[]) tupleEntry.getObject(0);

                HyperLogLog hll = HyperLogLog.Builder.build(serialized);
                if (merged == null) {
                    merged = hll;
                } else {
                    merged = (HyperLogLog) merged.merge(hll);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
