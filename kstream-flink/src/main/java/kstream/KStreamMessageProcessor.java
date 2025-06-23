package kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamMessageProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamMessageProcessor.class);
    private static final String STATUS_STORE_NAME = "core-status-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "core-status-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        final StreamsBuilder builder = new StreamsBuilder();

        // 1. Definir el store para guardar el estado del core como Integer
        StoreBuilder<KeyValueStore<String, Integer>> statusStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATUS_STORE_NAME),
                Serdes.String(),
                Serdes.Integer()
        );
        builder.addStateStore(statusStoreBuilder);

        // 2. Stream de mensajes técnicos (estado del core)
        KStream<String, String> technicalStream = builder.stream("topico_zero_tecnico");

        // Procesador para actualizar el estado del core (usando ProcessorSupplier correctamente)
        technicalStream.process(new ProcessorSupplier<String, String>() {
            @Override
            public Processor<String, String> get() {
                return new Processor<String, String>() {
                    private KeyValueStore<String, Integer> statusStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.statusStore = (KeyValueStore<String, Integer>) context.getStateStore(STATUS_STORE_NAME);
                    }

                    @Override
                    public void process(String key, String value) {
                        try {
                            JSONObject json = new JSONObject(value);
                            int coreStatus = json.getInt("system_core_up");
                            statusStore.put("core_status", coreStatus);
                            LOG.info("Estado del core actualizado a: {}", coreStatus == 1 ? "ACTIVO" : "INACTIVO");
                        } catch (Exception e) {
                            LOG.error("Error al procesar mensaje técnico: {}", value, e);
                        }
                    }

                    @Override
                    public void close() {}
                };
            }
        }, STATUS_STORE_NAME);

        // 3. Stream de mensajes principales
        KStream<String, String> messageStream = builder.stream("topico_origen");

        // Procesar mensajes según estado del core
        messageStream.transformValues(new ValueTransformerWithKeySupplier<String, String, String>() {
            @Override
            public ValueTransformerWithKey<String, String, String> get() {
                return new ValueTransformerWithKey<String, String, String>() {
                    private KeyValueStore<String, Integer> statusStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.statusStore = (KeyValueStore<String, Integer>) context.getStateStore(STATUS_STORE_NAME);
                    }

                    @Override
                    public String transform(String key, String value) {
                        Integer coreStatus = statusStore.get("core_status");
                        
                        if (coreStatus == null) {
                            coreStatus = 0;
                            statusStore.put("core_status", 0);
                        }

                        if (coreStatus == 1) {
                            LOG.info("Core ACTIVO - Enviando mensaje: {}", value);
                            return value;
                        } else {
                            LOG.debug("Core INACTIVO - Descartando mensaje: {}", value);
                            return null;
                        }
                    }

                    @Override
                    public void close() {}
                };
            }
        }, STATUS_STORE_NAME)
        .filter((key, value) -> value != null)
        .to("topico_destino");

        final Topology topology = builder.build();
        LOG.info("Topología:\n{}", topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Manejar cierre
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            LOG.error("Error en el procesamiento de streams", e);
            System.exit(1);
        }
        System.exit(0);
    }
}