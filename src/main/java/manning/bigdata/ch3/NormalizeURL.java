package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.DataUnit;
import manning.bigdata.swa.PageID;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 13:30
 * To change this template use File | Settings | File Templates.
 */
public class NormalizeURL extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Data data = ((Data) functionCall.getArguments().getObject(0)).deepCopy();
        DataUnit du = data.getDataunit();

        if (du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
            normalize(du.getPage_view().getPage());
        }
        functionCall.getOutputCollector().add(new Tuple(data));;
    }

    private void normalize(PageID page) {
        if (page.getSetField() == PageID._Fields.URL) {
            String urlStr = page.getUrl();
            try {
                URL url = new URL(urlStr);
                page.setUrl(url.getProtocol() + "://" + url.getHost() + url.getPath());
            } catch (MalformedURLException e) {
            }
        }
    }
}
