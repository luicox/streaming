package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlinkMessageProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMessageProcessor.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configuración de checkpointing (esencial para no reprocesar)
        env.enableCheckpointing(5000); // Checkpoint cada 5 segundos
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        String bootstrapServers = "localhost:9092";
        String sourceTopic = "topico_origen";
        String technicalTopic = "topico_zero_tecnico";
        String destinationTopic = "topico_destino";
        String consumerGroup = "flink_processor_group";

        // Configuración de KafkaSource para commit automático de offsets
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest()) // Cambiado a latest para no reprocesar
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .setProperty("isolation.level", "read_committed")
                .build();

        KafkaSource<String> technicalSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(technicalTopic)
                .setGroupId(consumerGroup + "_technical")
                .setStartingOffsets(OffsetsInitializer.latest()) // Cambiado a latest
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "true")
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(destinationTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        DataStream<String> messageStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source")
                .keyBy(value -> "static_key");

        DataStream<Boolean> technicalStream = env.fromSource(
                technicalSource,
                WatermarkStrategy.noWatermarks(),
                "Technical Kafka Source")
                .map(new MapFunction<String, Boolean>() {
                    @Override
                    public Boolean map(String value) {
                        try {
                            JSONObject json = new JSONObject(value);
                            return json.getInt("system_core_up") == 1;
                        } catch (Exception e) {
                            LOG.error("Error al parsear mensaje técnico: {}", value, e);
                            return false;
                        }
                    }
                }).keyBy(value -> "static_key");

        DataStream<String> processedStream = messageStream
                .connect(technicalStream)
                .process(new ConditionalForwardProcessFunction());

        processedStream.sinkTo(sink);

        env.execute("Flink Message Processor - Version con checkpointing");
    }

    private static class ConditionalForwardProcessFunction
            extends KeyedCoProcessFunction<String, String, Boolean, String> {

        private transient ValueState<List<String>> messageBuffer;
        private transient ValueState<Boolean> systemCoreUp;
        private transient ValueState<Long> lastProcessedOffset;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            messageBuffer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("messageBuffer", 
                            TypeInformation.of(new TypeHint<List<String>>(){}))
            );
            
            systemCoreUp = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("systemCoreUp", 
                            TypeInformation.of(Boolean.class), false)
            );
            
            lastProcessedOffset = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastProcessedOffset",
                            TypeInformation.of(Long.class))
            );
        }

        @Override
        public void processElement1(String message, Context ctx, Collector<String> out) throws Exception {
            // Verificar si este mensaje ya fue procesado
            Long currentOffset = ctx.timestamp(); // Usamos el timestamp como offset aproximado
            if (lastProcessedOffset.value() != null && currentOffset <= lastProcessedOffset.value()) {
                LOG.debug("Mensaje ya procesado (offset {}), ignorando", currentOffset);
                return;
            }

            List<String> buffer = messageBuffer.value();
            if (buffer == null) {
                buffer = new ArrayList<>();
            }

            buffer.add(message);
            messageBuffer.update(buffer);
            lastProcessedOffset.update(currentOffset);

            if (Boolean.TRUE.equals(systemCoreUp.value())) {
                flushBuffer(out);
            }
        }

        @Override
        public void processElement2(Boolean isSystemUp, Context ctx, Collector<String> out) throws Exception {
            systemCoreUp.update(isSystemUp);
            if (isSystemUp) {
                flushBuffer(out);
            }
        }

        private void flushBuffer(Collector<String> out) throws Exception {
            List<String> buffer = messageBuffer.value();
            if (buffer != null && !buffer.isEmpty()) {
                for (String msg : buffer) {
                    out.collect(msg);
                }
                messageBuffer.update(new ArrayList<>());
            }
        }
    }
}