package manning.bigdata.ch3;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 04.03.14
 * Time: 22:03
 * To change this template use File | Settings | File Templates.
 */
public interface FieldStructure {
    public boolean isValidTarget(String[] dirs);
    public void fillTarget(List<String> ret, Object val);
}

