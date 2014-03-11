package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 15:44
 * To change this template use File | Settings | File Templates.
 */
public class AnalyzeVisits extends CascalogBuffer {
    private static final int VISIT_LENGTH_SECS = 60 * 30;

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
        int bounces = 0;
        int visits = 0;
        Integer lastTime = null;
        int numInCurrVisit = 0;
        while (it.hasNext()) {
            TupleEntry tuple = it.next();
            int timeSecs = tuple.getInteger(0);
            if (lastTime == null  || (timeSecs - lastTime) > VISIT_LENGTH_SECS) {
                visits++;
                if (numInCurrVisit == 1) {
                    bounces++;
                }
                numInCurrVisit = 0;
            }
            numInCurrVisit++;
            lastTime = timeSecs;
        }

        if (numInCurrVisit == 1) {
            bounces++;
        }
        bufferCall.getOutputCollector().add(new Tuple(visits, bounces));
    }
}
