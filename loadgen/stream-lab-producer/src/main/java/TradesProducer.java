import com.tradestream.Trade;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class TradesProducer {
    public static void main(String[] args) {
        Properties props = getProperties();
        Map<String, Object> labProps = getYamlProps();
        String[] symbols = getSymbols(labProps);
        String[] sides = new String[]{"BUY", "SELL"};
        int countMessages = (int) labProps.get("totalMessages");
        int recordsRate = (int) labProps.get("recordsRate");
        int latePct = (int) labProps.get("latePercent");
        int maxLateNess = (int) labProps.get("maxLatenessMs");
        String topic = labProps.get("topicName").toString();

        int randomSeed = (int) labProps.get("randomSeed");
        Random seededRand = new Random(randomSeed);

        try (Producer<String, Trade> kafkaProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < countMessages; i++) {
                String sym = symbols[i % symbols.length];
                double price = getPrice(1.0, 1000.0, 3, seededRand);
                int qty = seededRand.nextInt(1, 1000);
                String side = sides[seededRand.nextInt(sides.length)];
                long eventTime = getEventTime(i, latePct, maxLateNess, seededRand);
                Trade trade = Trade.newBuilder()
                        .setSymbol(sym)
                        .setPrice(price)
                        .setQuantity(qty)
                        .setSide(side)
                        .setEventTime(eventTime)
                        .build();
                kafkaProducer.send(new ProducerRecord<>(topic, trade.getSymbol().toString(), trade));

            }
        }
    }

    private static long getEventTime(int msgNumber, int latePct, long maxLateNess, Random seededRandom) {
        long now = System.currentTimeMillis();
        if(seededRandom.nextInt(100) < latePct) {
            now = now - seededRandom.nextLong(0, maxLateNess);
        }
        return now;
    }

    private static String @NonNull [] getSymbols(Map<String, Object> labProps) {
        return ((List<?>) labProps.get("symbols")).stream().map(Object::toString).toArray(String[]::new);
    }

    private static Map<String, Object> getYamlProps() {
        Yaml yaml= new Yaml();
        Map<String, Object> yMap = null;
        try (InputStream inputStream = yaml.getClass()
                .getClassLoader()
                .getResourceAsStream("lab.yaml")) {
            yMap = yaml.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return yMap;
    }

    private static @NonNull Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        return props;
    }

    private static double getPrice(double low, double high, int decimalPlaces, Random seededRandom) {
        int base = 10;
        double d = Math.pow(base, decimalPlaces);
        double rand = seededRandom.nextDouble(low, high);
        return Math.round(rand * d) / d;
    }
}
