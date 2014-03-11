package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.EquivEdge;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 13:40
 * To change this template use File | Settings | File Templates.
 */
public class EdgifyEquiv extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Data data = (Data) functionCall.getArguments().getObject(0);
        EquivEdge equiv = data.getDataunit().getEquiv();
        functionCall.getOutputCollector().add(new Tuple(equiv.getId1(), equiv.getId2()));
    }
}
