package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.PersonID;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 14:23
 * To change this template use File | Settings | File Templates.
 */
public class MakeNormalizedPageview extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        PersonID newId = (PersonID) functionCall.getArguments().getObject(0);
        Data data = ((Data) functionCall.getArguments().getObject(1)).deepCopy();
        if (newId != null) {
            data.getDataunit().getPage_view().setPerson(newId);
        }
        functionCall.getOutputCollector().add(new Tuple(data));
    }
}
