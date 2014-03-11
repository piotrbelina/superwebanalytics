package manning.bigdata.ch3;

import manning.bigdata.swa.Data;

import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 02.03.14
 * Time: 19:30
 * To change this template use File | Settings | File Templates.
 */
public class DataPailStructure extends ThriftPailStructure<Data> {
    @Override
    protected Data createThriftObject() {
        return new Data();
    }

    @Override
    public boolean isValidTarget(String... strings) {
        return true;
    }

    @Override
    public List<String> getTarget(Data data) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Class getType() {
        return Data.class;
    }
}
