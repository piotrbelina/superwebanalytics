package manning.bigdata.main;

import manning.bigdata.kafka.StreamingNewDataToQueue;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 11.03.14
 * Time: 23:03
 * To change this template use File | Settings | File Templates.
 */
public class StreamDataToQueue {
    public static void main(String[] args) {
        String kafkaServer = "";
        String topic = "";

        StreamingNewDataToQueue streaming = new StreamingNewDataToQueue(kafkaServer, topic);
        String dateStart = "";
        String dateEnd = "";
        String batch = "";
        String factType = "";
        streaming.generateAndStreamingDataToQueue(dateStart, dateEnd, batch, factType);
    }
}
