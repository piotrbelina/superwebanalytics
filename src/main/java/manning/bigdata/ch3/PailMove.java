/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 01.03.14
 * Time: 17:14
 * To change this template use File | Settings | File Templates.
 */

package manning.bigdata.ch3;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascalog.ops.IdentityBuffer;
import cascalog.ops.RandLong;
import clojure.lang.Keyword;
import clojure.lang.PersistentStructMap;
import com.backtype.cascading.tap.PailTap;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.DataUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.*;

public class PailMove {
    public static final String TEMP_DIR = "/tmp/swa";
    public static final String NEW_DATA_LOCATION = "/tmp/newData";
    public static final String MASTER_DATA_LOCATION = "/tmp/masterData";
    public static final String SNAPSHOT_LOCATION = "/tmp/swa/newDataSnapshot";
    public static final String SHREDDED_DATA_LOCATION = "/tmp/swa/shredded";
    public static final String NORMALIZED_URLS_LOCATION = "/tmp/swa/normalized_urls";
    public static final String NORMALIZED_PAGEVIEW_LOCATION = "/tmp/swa/normalized_pageview_users";
    public static final String UNIQUE_PAGEVIEW_LOCATION = "/tmp/swa/unique_pageviews";

    public static void mergeData(String masterDir, String updateDir) throws IOException {
        Pail target = new Pail(masterDir);
        Pail source = new Pail(updateDir);
        target.absorb(source);
        target.consolidate();
    }

    public static void simpleIO() throws IOException {
        Pail pail = Pail.create("/tmp/mypail");
        Pail.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject(new byte[] {1,2,3});
        os.writeObject(new byte[] {1,2,3,4});
        os.writeObject(new byte[] {1,2,3,4,5});
        os.close();
    }

//    public static void writeLogins() throws IOException {
//        Pail<Login> loginPail = Pail.create("/tmp/updates", new LoginPailStructure());
//        Pail.TypedRecordOutputStream out = loginPail.openWrite();
//        out.writeObject(new Login("charlie", 1352671234));
//        out.writeObject(new Login("delta", 1352679456));
//        out.close();
//    }
//
//    public static void readLogins(String path) throws IOException {
//        Pail<Login> loginPail = new Pail<Login>(path);
//        System.out.println(path);
//        for (Login l : loginPail) {
//            System.out.println(l.userName + " " + l.loginUnixTime);
//        }
//        System.out.println("");
//    }
//
//    public static void appendData() throws IOException {
//        Pail<Login> loginPail = new Pail<Login>("/tmp/logins");
//        Pail<Login> updatePail = new Pail<Login>("/tmp/updates");
//        loginPail.absorb(updatePail);
//    }
//
//    public static void partitionData() throws IOException {
//        Pail<Login> pail = Pail.create("/tmp/partitioned_logins",
//                new PartitionedLoginPailStructure());
//        Pail.TypedRecordOutputStream os = pail.openWrite();
//        os.writeObject(new Login("chris", 1352702020));
//        os.writeObject(new Login("david", 1352788472));
//        os.close();
//    }
//
//    public static void createCompressedPail() throws IOException {
//        Map<String, Object> options = new HashMap<String, Object>();
//        options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
//        options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);
//        LoginPailStructure struct = new LoginPailStructure();
//        Pail compressed = Pail.create("/tmp/compressed", new PailSpec("SequenceFile", options, struct));
//    }

    public static void setApplicationConf() throws IOException {
        Map conf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization,org.apache.hadoop.io.serializer.WritableSerialization";
        conf.put("io.serializations", sers);
        Api.setApplicationConf(conf);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(TEMP_DIR), true);
        fs.mkdirs(new Path(TEMP_DIR));
    }

    public static void ingest(Pail masterPail, Pail newDataPail) throws IOException {
        Pail snapshotPail = newDataPail.snapshot(SNAPSHOT_LOCATION);
        shred();
        consolidateAndAbsord(masterPail, new Pail(SHREDDED_DATA_LOCATION));
        newDataPail.deleteSnapshot(snapshotPail);
    }

    private static void consolidateAndAbsord(Pail masterPail, Pail shreddedPail) throws IOException {
        shreddedPail.consolidate();
        masterPail.absorb(shreddedPail);
    }

    public static PailTap attributeTap(String path, final DataUnit._Fields... fields) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.attrs = new List[] {
                new ArrayList<String>() {{
                    for (DataUnit._Fields field: fields) {
                        add("" + field.getThriftFieldId());
                    }
                }}
        };
        opts.spec = new PailSpec((PailStructure) new SplitDataPailStructure());
        return new PailTap(path, opts);
    }

    public static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec((PailStructure) new SplitDataPailStructure());
        return new PailTap(path, opts);
    }

    public static PailTap deserializeDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new DataPailStructure());
        return new PailTap(path, opts);
    }


    public static void shred() throws IOException {
        PailTap source = deserializeDataTap(SNAPSHOT_LOCATION);
        PailTap sink = splitDataTap(SHREDDED_DATA_LOCATION);

        Subquery reduced = new Subquery("?rand", "?data")
                .predicate(source, "_", "?data-in")
                .predicate(new RandLong())
                    .out("?rand")
                .predicate(new IdentityBuffer(), "?data-in")
                    .out("?data");

        Api.execute(sink, new Subquery("?data").predicate(reduced, "_", "?data"));
    }

    public static void normalizeURLs() {
        Tap masterDataset = splitDataTap(MASTER_DATA_LOCATION);
        Tap outTap = splitDataTap(NORMALIZED_URLS_LOCATION);
        Api.execute(outTap,
                new Subquery("?normalized")
                    .predicate(masterDataset, "_", "?raw")
                    .predicate(new NormalizeURL(), "?raw")
                        .out("?normalized")
        );
    }

    public static void initializeUserIdNormalization() {
        Tap equivs = attributeTap(NORMALIZED_URLS_LOCATION, DataUnit._Fields.EQUIV);

        Api.execute(Api.hfsSeqfile("/tmp/swa/equivs0"),
                new Subquery("?node1", "?node2")
                .predicate(equivs, "_", "?data")
                .predicate(new EdgifyEquiv(), "?node1", "?node2")
        );
    }

    public static Subquery iterationQuery(Tap source) {
        Subquery iterate = new Subquery("?b1", "?node1", "?node2", "?is-new")
                .predicate(source, "?n1", "?n2")
                .predicate(new BidirectionalEdge(), "?n1", "?n2")
                    .out("?b1", "?b2")
                .predicate(new IterateEdges(), "?b2")
                    .out("?node1", "node2", "?is-new");

//        iterate = (Subquery) Api.selectFields(iterate, new Fields("?node1", "?node2", "?is-new"));

        return iterate;
    }

    private static Tap hfsSeqfile(String fileName, String tapType) {
        PersistentStructMap psm = (PersistentStructMap) Api.hfsSeqfile(fileName);
        return (Tap) psm.get(Keyword.intern(tapType));
    }

    public static Tap userIdNormalizationIteration(int i) {
        Tap source = hfsSeqfile("/tmp/swa/equivs" + (i - 1), "source");
        Tap sink = hfsSeqfile("/tmp/swa/equivs" + i, "sink");
        Subquery iteration = new Subquery("?b1", "?node1", "?node2", "?is-new")
                .predicate(source, "?n1", "?n2")
                .predicate(new BidirectionalEdge(), "?n1", "?n2").out("?b1", "?b2")
                .predicate(new IterateEdges(), "?b2").out("?node1", "?node2", "?is-new");

        Subquery newEdgeSet = new Subquery("?node1", "?node2")
                .predicate(iteration, "_", "?node1", "?node2", "_")
                .predicate(Option.DISTINCT, true);

        Api.execute(sink, newEdgeSet);

        Tap progressEdgesSink = hfsSeqfile("/tmp/swa/equivs" + i + "-new", "sink");
        Subquery progressEdges = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", true);

        Api.execute(progressEdgesSink, progressEdges);

        return progressEdgesSink;
    }

    public static int userIdNormalizationLoop() throws IOException {
        int iter = 1;
        while (true) {
            Tap progressEdgesSink = userIdNormalizationIteration(iter);
            FlowProcess flowProcess = new HadoopFlowProcess(new JobConf());
            if (!flowProcess.openTapForRead(progressEdgesSink).hasNext()) {
                return iter;
            }
            iter++;
        }
    }

    public static void modifyPageViews(int iter) {
        Tap pageviews = attributeTap(NORMALIZED_URLS_LOCATION, DataUnit._Fields.PAGE_VIEW);
        Tap newIds = hfsSeqfile("/tmp/swa/equivs" + iter, "source");
        Tap result = splitDataTap(NORMALIZED_PAGEVIEW_LOCATION);

        Api.execute(result,
                new Subquery("?normalized-pageview")
                .predicate(newIds, "!!newId", "?person")
                .predicate(pageviews, "_", "?data")
                .predicate(new ExtractPageViewFields(), "?data")
                    .out("_", "?person", "_")
                .predicate(new MakeNormalizedPageview(), "!!newId", "?data")
                    .out("?normalized-pageview")
        );
    }

    public static void normalizeUserIds() throws IOException {
        initializeUserIdNormalization();
        int numIterations = userIdNormalizationLoop();
        modifyPageViews(numIterations);
    }

    public static void deduplicatePageviews() {
        Tap source = attributeTap(NORMALIZED_URLS_LOCATION, DataUnit._Fields.PAGE_VIEW);
        Tap outTap = splitDataTap(UNIQUE_PAGEVIEW_LOCATION);

        Api.execute(outTap,
                new Subquery("?data")
                    .predicate(source, "_", "?data")
                    .predicate(Option.DISTINCT, true)
        );
    }

    public static Subquery hourlyRollup() {
        Tap source = splitDataTap(UNIQUE_PAGEVIEW_LOCATION);
        return new Subquery("?url", "?hour-bucket", "?count")
                .predicate(source, "_", "?pageview")
                .predicate(new ExtractPageViewFields(), "?pageview")
                    .out("?url", "_", "?timestamp")
                .predicate(new ToHourBucket(), "?timestamp")
                    .out("?hour-bucket")
                .predicate(new Count(), "?count");
    }

    public static Subquery pageviewBatchView() {
        Subquery pageviews = new Subquery("?url", "?granularity", "?bucket", "?total-pageviews")
                .predicate(hourlyRollup(), "?url", "?hour-bucket", "?count")
                .predicate(new EmitGranularities(), "?hour-bucket")
                    .out("?granularity", "?bucket")
                .predicate(new Sum(), "?count").out("?total-pageviews");
        return pageviews;
    }

    public static void uniquesViews() {
        Tap source = splitDataTap(UNIQUE_PAGEVIEW_LOCATION);

        Subquery hourlyUniques = new Subquery("?url", "?hour-bucket", "?hyper-log-log")
                .predicate(source, "?pageview")
                .predicate(new ExtractPageViewFields(), "?pageview")
                    .out("?url", "?user", "?timestamp")
                .predicate(new ToHourBucket(), "?timestamp")
                    .out("?hour-bucket")
                .predicate(new ConstructHyperLogLog(), "?user")
                    .out("?hyper-log-log")
            ;

        Subquery uniques = new Subquery("?url", "?granularity", "?bucket", "aggregate-hll")
                .predicate(hourlyUniques, "?url", "?hour-bucket", "?hourly-hll")
                .predicate(new EmitGranularities(), "?hour-bucket")
                    .out("?granularity", "?bucket")
                .predicate(new MergeHyperLogLog(), "?hourly-hll")
                    .out("?aggregate-hll");
    }

    public static Subquery bouncesView() {
        Tap source = splitDataTap(UNIQUE_PAGEVIEW_LOCATION);

        Subquery userVisits = new Subquery("?domain", "?user", "?num-user-visits", "?num-user-bounces")
                .predicate(source, "_", "?pageview")
                .predicate(new ExtractPageViewFields(), "?pageview")
                    .out("?url", "?user", "?timestamp")
                .predicate(new ExtractDomain(), "?url")
                    .out("?domain")
                .predicate(Option.SORT, "?timestamp")
                .predicate(new AnalyzeVisits(), "?timestamp")
                    .out("?num-user-visits", "?num-user-bounces");

        Subquery bounces = new Subquery("?domain", "?num-visits", "?num-bounces")
                .predicate(userVisits, "?domain", "_", "?num-user-visits", "?num-user-bounces")
                .predicate(new Sum(), "?num-user-visits")
                    .out("?num-visits")
                .predicate(new Sum(), "?num-user-bounces")
                    .out("?num-bounces");

        return bounces;
    }

    public static void main(String args[]) throws Exception {
        setApplicationConf();

        LocalFileSystem fs = new LocalFileSystem();

        Pail newDataPail;
        Pail masterPail;
        if (!fs.exists(new Path(NEW_DATA_LOCATION))) {
            newDataPail = Pail.create(FileSystem.getLocal(new Configuration()), NEW_DATA_LOCATION, new DataPailStructure());
        } else {
            newDataPail = new Pail<Data>(NEW_DATA_LOCATION);
        }

        if (!fs.exists(new Path(MASTER_DATA_LOCATION))) {
            masterPail = Pail.create(FileSystem.getLocal(new Configuration()), MASTER_DATA_LOCATION, new SplitDataPailStructure());
        } else {
            masterPail = new Pail<Data>(MASTER_DATA_LOCATION);
        }

        Pail.TypedRecordOutputStream out = newDataPail.openWrite();

        out.writeObject(GenerateData.getPageView(1, "http://www.strona.pl/a.html", 1394380536, 13943805361L));
        out.writeObject(GenerateData.getPageView(1, "http://www.strona.pl/b.html", 1394380944, 13943805360L));

        out.writeObject(GenerateData.getPageView(2, "http://www.strona.pl/a.html", 1394380536, 13943805368L));

        out.writeObject(GenerateData.getPageView(3, "http://www.strona.pl/a.html", 1394380536, 13943805365L));
        out.writeObject(GenerateData.getPageView(3, "http://www.strona.pl/b.html", 1394380530, 13943805367L));

        out.writeObject(GenerateData.getPageView(4, "http://www.strona.pl/a.html", 1394310536, 13943105362L));
        out.writeObject(GenerateData.getPageView(4, "http://www.strona.pl/b.html", 1394380944, 13943805352L));

        out.close();

        ingest(masterPail, newDataPail);

        normalizeURLs();

//        normalizeUserIds();

        deduplicatePageviews();

        pageviewBatchView();

        Api.execute(new StdoutTap(),bouncesView());

        bouncesView();
    }

}
