package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.PageID;
import manning.bigdata.swa.PageViewEdge;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 14:19
 * To change this template use File | Settings | File Templates.
 */
public class ExtractPageViewFields extends CascalogFunction {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Data data = (Data) functionCall.getArguments().getObject(0);
        PageViewEdge pageview = data.getDataunit().getPage_view();
        if (pageview.getPage().getSetField() == PageID._Fields.URL) {
            functionCall.getOutputCollector().add(
                    new Tuple(pageview.getPage().getUrl(),
                            pageview.getPerson(),
                            data.getPedigree().getTrue_as_of_secs())
            );
        }
    }
}
