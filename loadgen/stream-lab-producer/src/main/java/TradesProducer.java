import com.tradestream.Trade;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.jspecify.annotations.NonNull;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TradesProducer {
    public static void main(String[] args) {
        Properties props = getProperties();
        Map<String, Object> labProps = getYamlProps();
        String[] symbols = getSymbols(labProps);
        String[] sides = new String[]{"BUY", "SELL"};
        int countMessages = (int) labProps.get("totalMessages");
        int recordsRate = (int) labProps.get("recordsRate");
        long sleep = 1000/recordsRate;
        int latePct = (int) labProps.get("latePercent");
        int maxLateNess = (int) labProps.get("maxLatenessMs");
        String topic = labProps.get("topicName").toString();

        int lateCnt = countMessages * latePct/100;
        System.out.println(lateCnt);
        try (Producer<Integer, Trade> kafkaProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < countMessages; i++) {
                String sym = symbols[countMessages % symbols.length];
                double price = getPrice(1.0, 1000.0, 3);
                int qty = ThreadLocalRandom.current().nextInt(1, 1000);
                String side = sides[Math.toIntExact(System.currentTimeMillis() % sides.length)];
                long eventTime = getEventTime(i, lateCnt, maxLateNess);
                Trade trade = Trade.newBuilder()
                        .setSymbol(sym)
                        .setPrice(price)
                        .setQuantity(qty)
                        .setSide(side)
                        .setEventTime(eventTime)
                        .build();
                kafkaProducer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), 1, trade));
                Thread.sleep(sleep);

            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getEventTime(int msgNumber, int lateCnt, long maxLateNess) {
        long now = System.currentTimeMillis();
        if(msgNumber % lateCnt == 0) {
            now = now - ThreadLocalRandom.current().nextLong(0, maxLateNess);
            System.out.println(msgNumber +" --> LATE");
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
//            System.out.println(yMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return yMap;
    }

    private static @NonNull Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        return props;
    }

    private static double getPrice(double low, double high, int decimalPlaces) {
        int base = 10;
        double d = Math.pow(base, decimalPlaces);
        double rand = ThreadLocalRandom.current().nextDouble(low, high);
        return Math.round(rand * d) / d;
    }
}
