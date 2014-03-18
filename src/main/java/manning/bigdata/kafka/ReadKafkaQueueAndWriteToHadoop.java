package manning.bigdata.kafka;

import com.backtype.hadoop.pail.Pail;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import manning.bigdata.swa.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 12.03.14
 * Time: 22:21
 * To change this template use File | Settings | File Templates.
 */
public class ReadKafkaQueueAndWriteToHadoop implements Runnable {
    private final KafkaStream stream;
    private final int thread;
    private final String hdfsPath;

    public ReadKafkaQueueAndWriteToHadoop(KafkaStream stream, int thread, String hdfsPath) {
        this.stream = stream;
        this.thread = thread;
        this.hdfsPath = hdfsPath;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        String messages = "";
        while (it.hasNext()) {
            messages = new String(it.next().message());
            try {
                writeToHadoop(messages);
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeToHadoop(String jsonMessages) throws ParseException, IOException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(jsonMessages);
        JSONObject jsonObject = (JSONObject) obj;
        String messageType = (String) jsonObject.get("messagetype");
        Pail hadoopPail = new Pail(hdfsPath);
        Pail.TypedRecordOutputStream os = hadoopPail.openWrite();

        Data data = decodeJsonMessage(jsonObject, messageType);
        os.writeObject(data);
    }

    private Data decodeJsonMessage(JSONObject jsonObject, String messageType) {
        Data data = null;

        if (messageType.equals("page")) {
            data = decodePage(jsonObject);
        } else if (messageType.equals("person")) {
            data = decodePerson(jsonObject);
        } else if (messageType.equals("equiv")) {
            data = decodeEquiv(jsonObject);
        } else if (messageType.equals("pageview")) {
            data = decodePageView(jsonObject);
        }
        return data;
    }

    private Data decodePage(JSONObject jsonObject) {
        String pedigree = (String) jsonObject.get("pedigree");
        String url = (String) jsonObject.get("url");
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Data decodePerson(JSONObject jsonObject) {
        System.out.println(jsonObject);
        String pedigree = (String) jsonObject.get("pedigree");
        String personId = (String) jsonObject.get("personid");
        String gender = (String) jsonObject.get("gender");

        GenderType genderType = null;
        if (gender.equals("MALE")) {
            genderType = GenderType.MALE;
        } else {
            genderType = GenderType.FEMALE;
        }

        String fullname = (String) jsonObject.get("fullname");
        String city = (String) jsonObject.get("city");
        String state = (String) jsonObject.get("state");
        String country = (String) jsonObject.get("country");

        PersonID personID = new PersonID();
        if (personId.startsWith("cookie")) {
            personID.setCookie(personId);
        } else {
            personID.setUser_id(Long.parseLong(personId));
        }

        PersonPropertyValue personPropertyValue = new PersonPropertyValue();
        Location location = new Location();
        location.setCity(city);
        location.setState(state);
        location.setCountry(country);
        personPropertyValue.setGender(genderType);
        personPropertyValue.setLocation(location);
        personPropertyValue.setFull_name(fullname);

        PersonProperty personProperty = new PersonProperty();
        personProperty.setProperty(personPropertyValue);
        personProperty.setId(personID);

        DataUnit dataUnit = new DataUnit();
        dataUnit.setPerson_property(personProperty);

        return getData(pedigree, dataUnit);
    }

    private Data getData(String timestamp, DataUnit dataUnit) {
        Pedigree pedigree = new Pedigree();
        pedigree.setTrue_as_of_secs(Integer.parseInt(timestamp));

        Data data = new Data();
        data.setPedigree(pedigree);
        data.setDataunit(dataUnit);

        return data;
    }

    private Data decodeEquiv(JSONObject jsonObject) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Data decodePageView(JSONObject jsonObject) {
        return null;
    }
}
