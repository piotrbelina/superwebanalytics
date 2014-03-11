package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 14:40
 * To change this template use File | Settings | File Templates.
 */
public class ToHourBucket extends CascalogFunction{
    private static final int HOUR_IN_SECS = 60 * 60;

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        int timestamp = functionCall.getArguments().getInteger(0);
        int hourBucket = timestamp / HOUR_IN_SECS;
        functionCall.getOutputCollector().add(new Tuple(hourBucket));
    }
}
