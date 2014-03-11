package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import manning.bigdata.swa.PersonID;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 13:46
 * To change this template use File | Settings | File Templates.
 */
public class IterateEdges extends CascalogBuffer {
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        PersonID grouped = (PersonID) bufferCall.getGroup().getObject(0);
        TreeSet<PersonID> allIds = new TreeSet<PersonID>();
        allIds.add(grouped);

        Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
        while (it.hasNext()) {
            allIds.add((PersonID) it.next().getObject(0));
        }

        Iterator<PersonID> allIdsIt = allIds.iterator();
        PersonID smallest = allIdsIt.next();
        boolean progress = allIds.size() > 2 && !grouped.equals(smallest);

        while (allIdsIt.hasNext()) {
            PersonID id = allIdsIt.next();
            bufferCall.getOutputCollector().add(new Tuple(smallest, id, progress));
        }
    }
}
