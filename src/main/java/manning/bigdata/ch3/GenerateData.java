package manning.bigdata.ch3;

import com.backtype.hadoop.pail.Pail;
import manning.bigdata.swa.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 09.03.14
 * Time: 16:47
 * To change this template use File | Settings | File Templates.
 */
public class GenerateData {

    public static Data getPageView(long userId, String url, int timestamp, long nonce)
    {
        Pedigree pedigree = new Pedigree(timestamp);
        PersonID person = new PersonID(PersonID._Fields.USER_ID, userId);
        PageID page = new PageID(PageID._Fields.URL, url);
        PageViewEdge pageview = new PageViewEdge(person, page, nonce);
        DataUnit dataunit = new DataUnit(DataUnit._Fields.PAGE_VIEW, pageview);
        return new Data(pedigree, dataunit);
    }

    public static void main(String[] args) throws IOException {
        Pail newDataPail = Pail.create(FileSystem.getLocal(new Configuration()), PailMove.NEW_DATA_LOCATION, new SplitDataPailStructure());


        Pail.TypedRecordOutputStream out = newDataPail.openWrite();

        out.writeObject(getPageView(1, "http://www.strona.pl/a.html", 1394380536, 13943805361L));
        out.writeObject(getPageView(1, "http://www.strona.pl/b.html", 1394380944, 13943805360L));

        out.close();
    }
}
