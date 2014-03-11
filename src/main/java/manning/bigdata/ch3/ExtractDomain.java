package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 15:18
 * To change this template use File | Settings | File Templates.
 */
public class ExtractDomain extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String urlStr = functionCall.getArguments().getString(0);
        try {
            URL url = new URL(urlStr);
            functionCall.getOutputCollector().add(new Tuple(url.getAuthority()));
        } catch (MalformedURLException e) {
        }
    }
}
