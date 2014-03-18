package manning.bigdata.main;

import manning.bigdata.kafka.StreamingNewDataToQueue;

import java.text.ParseException;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 11.03.14
 * Time: 23:03
 * To change this template use File | Settings | File Templates.
 */
public class StreamDataToQueue {
    public static void main(String[] args) throws ParseException {
        String kafkaServer = "localhost:9092";
        String topic = "swa";

        StreamingNewDataToQueue streaming = new StreamingNewDataToQueue(kafkaServer, topic);
        String dateStart = "01.01.2013|10:20:20";
        String dateEnd = "01.01.2014|10:20:20";
        String batch = "";
        String factType = "";
        streaming.generateAndStreamingDataToQueue(dateStart, dateEnd, batch, factType);
    }
}
