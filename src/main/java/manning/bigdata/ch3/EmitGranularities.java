package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 14:46
 * To change this template use File | Settings | File Templates.
 */
public class EmitGranularities extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        int hourBucket = functionCall.getArguments().getInteger(0);
        int dayBucket = hourBucket / 24;
        int weekBucket = dayBucket / 7;
        int monthBucket = weekBucket / 28;

        functionCall.getOutputCollector().add(new Tuple("h", hourBucket));
        functionCall.getOutputCollector().add(new Tuple("d", dayBucket));
        functionCall.getOutputCollector().add(new Tuple("w", weekBucket));
        functionCall.getOutputCollector().add(new Tuple("m", monthBucket));
    }
}
