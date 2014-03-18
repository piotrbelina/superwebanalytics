package manning.bigdata.main;

import manning.bigdata.ch3.PailMove;
import manning.bigdata.kafka.StreamingNewDataToQueue;
import manning.bigdata.kafka.StreamingQueueToHadoop;

import java.text.ParseException;

/**
 * Created by bela on 18.03.14.
 */
public class StreamQueueToHadoop {
    public static void main(String[] args) throws ParseException {
        String zookeeper = "localhost:2181";
        String topic = "swa";

        int threads = 1;
        String hdfsPath = PailMove.NEW_DATA_LOCATION;

        StreamingQueueToHadoop streaming = new StreamingQueueToHadoop(hdfsPath, zookeeper, topic, threads);
        streaming.startStreaming();
    }
}
