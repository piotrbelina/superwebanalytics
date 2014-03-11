package manning.bigdata.ch3;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 04.03.14
 * Time: 22:03
 * To change this template use File | Settings | File Templates.
 */
public class EdgeStructure implements FieldStructure {
    @Override
    public boolean isValidTarget(String[] dirs) {
        return  true;
    }

    @Override
    public void fillTarget(List<String> ret, Object val) {}
}
