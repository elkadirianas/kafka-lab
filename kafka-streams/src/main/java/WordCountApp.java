import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced; // <--- NEW IMPORT

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-wordcount-app");
        // Use the proper StreamsConfig key for bootstrap servers
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka-broker1:9092,kafka-broker2:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // Note: The below line is redundant and can be removed, as BOOTSTRAP_SERVERS_CONFIG handles this.
        // props.put("bootstrap.servers", "kafka-broker1:9092"); 

        StreamsBuilder builder = new StreamsBuilder();

        // Read raw input logs
        KStream<String, String> textLines = builder.stream("input-logs");

        // 🔍 Debug
        textLines.peek((k, v) -> System.out.println("➡️ Received: " + v));

        KTable<String, Long> wordCounts = textLines
            // split line into words
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            .filter((k, v) -> v != null && !v.isEmpty())  
            .peek((k, v) -> System.out.println(" Word: " + v))
            // group words as keys
            .groupBy((key, word) -> word)
            // KTable<String, Long> is created here
            .count(Materialized.with(Serdes.String(), Serdes.Long())); 

        // Output aggregated table results
        wordCounts
            .toStream() // Convert KTable<String, Long> to KStream<String, Long>
            .peek((k,v) -> System.out.println(" Count: " + k + " = " + v))
            
            // ⭐️ NEW STEP: Convert the Long value to a String
            // KStream<String, Long> -> KStream<String, String>
            .mapValues(Object::toString) 
            
            // ⭐️ NEW ARGUMENT: Specify the String Serdes for the output topic
            .to("word-counts", Produced.with(Serdes.String(), Serdes.String())); 
            
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
