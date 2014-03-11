package manning.bigdata.kafka;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 11.03.14
 * Time: 23:04
 * To change this template use File | Settings | File Templates.
 */
public class StreamingNewDataToQueue {
    String kafkaServer;
    String topic;

    public StreamingNewDataToQueue(String kafkaServer, String topic) {
        this.kafkaServer = kafkaServer;
        this.topic = topic;
    }

    public void generateAndStreamingDataToQueue(String dateStart, String dateEnd, String batch, String factType) {
        setupKafka();
    }

    private void setupKafka() {
        Properties props = new Properties();

        props.put("metadata.broker.list", this.kafkaServer);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");


        ProducerConfig config = new ProducerConfig(props);
    }
}
