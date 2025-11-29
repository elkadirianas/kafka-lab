import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

public class UppercaseApp {
    public static void main(String[] args) {

        System.out.println("🔧 Starting UppercaseApp...");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-uppercase-app");
	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        System.out.println("📥 Building stream topology...");

        KStream<String, String> input = builder.stream("input-logs");

        input.peek((k,v) -> System.out.println("➡️ Received: " + v))
             .mapValues(value -> {
                 String up = value.toUpperCase();
                 System.out.println("🔼 Transformed: " + up);
                 return up;
             })
             .peek((k,v) -> System.out.println("📤 Sending: " + v))
             .to("output-logs");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        System.out.println("🚀 Starting Kafka Streams...");
        streams.start();
        System.out.println("✔️ UppercaseApp is running!");
    }
}

