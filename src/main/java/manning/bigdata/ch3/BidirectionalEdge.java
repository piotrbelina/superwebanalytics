package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 13:45
 * To change this template use File | Settings | File Templates.
 */
public class BidirectionalEdge extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Object node1 = functionCall.getArguments().getObject(0);
        Object node2 = functionCall.getArguments().getObject(1);

        if (!node1.equals(node2)) {
            functionCall.getOutputCollector().add(new Tuple(node1, node2));
            functionCall.getOutputCollector().add(new Tuple(node2, node1));
        }
    }
}
