package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Consumidor del stream de eventos de negocio (Entrega 3: el "streaming de
// datos"). Recorre cada shard en orden y alimenta la misma agregación que ya
// usaba el consumidor de Kafka: las ventas del panel salen del stream.
//
// Política de errores del stream (proporcional a la falla, como en las colas):
//  - evento ilegible o sin datos esenciales: se REGISTRA completo en el log y
//    se salta (el stream conserva el registro: puede reprocesarse a mano);
//  - falla transitoria (base caída): se registra y el bucle sigue; el procesar
//    es idempotente por pedidoId, así que un replay posterior no duplica ventas.
// El checkpoint avanza con el shard: al reiniciar se retoma desde el último
// offset visto (y con idempotencia, repetir nunca cuenta dos veces).
@Component
public class KinesisEventosConsumer {

    private static final Logger log = LoggerFactory.getLogger(KinesisEventosConsumer.class);

    private final KinesisClient kinesisClient;
    private final ObjectMapper objectMapper;
    private final EstadisticasEventosHandler handler;
    private final String stream;
    private final boolean habilitado;

    // Último sequence number procesado por shard (checkpoint en memoria)
    private final Map<String, String> checkpoints = new ConcurrentHashMap<>();

    private Thread hilo;
    private volatile boolean corriendo;

    public KinesisEventosConsumer(KinesisClient kinesisClient,
                                  ObjectMapper objectMapper,
                                  EstadisticasEventosHandler handler,
                                  @Value("${app.kinesis.stream:eventos-negocio}") String stream,
                                  @Value("${app.kinesis.habilitado:false}") boolean habilitado) {
        this.kinesisClient = kinesisClient;
        this.objectMapper = objectMapper;
        this.handler = handler;
        this.stream = stream;
        this.habilitado = habilitado;
    }

    @PostConstruct
    public void iniciar() {
        if (!habilitado) {
            log.info("Consumidor de Kinesis deshabilitado (app.kinesis.habilitado=false)");
            return;
        }
        corriendo = true;
        hilo = new Thread(this::bucle, "kinesis-eventos-consumer");
        hilo.setDaemon(true);
        hilo.start();
        log.info("Consumidor del stream '{}' arrancado", stream);
    }

    @PreDestroy
    public void detener() {
        corriendo = false;
        if (hilo != null) {
            hilo.interrupt();
        }
    }

    private void bucle() {
        while (corriendo) {
            try {
                for (String shard : shards()) {
                    procesarShard(shard);
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Falla transitoria (stream o red): se registra y se reintenta
                // en la siguiente vuelta; nunca se cae el consumidor
                log.error("Fallo recuperable leyendo el stream '{}': {}", stream, e.getMessage());
                dormirSeguro(2000);
            }
        }
    }

    private void procesarShard(String shard) {
        String desde = checkpoints.get(shard);
        String iterator;
        if (desde == null) {
            iterator = kinesisClient.getShardIterator(GetShardIteratorRequest.builder()
                    .streamName(stream).shardId(shard)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build()).shardIterator();
        } else {
            iterator = kinesisClient.getShardIterator(GetShardIteratorRequest.builder()
                    .streamName(stream).shardId(shard)
                    .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .startingSequenceNumber(desde)
                    .build()).shardIterator();
        }

        while (iterator != null && corriendo) {
            GetRecordsResponse respuesta = kinesisClient.getRecords(
                    GetRecordsRequest.builder().shardIterator(iterator).limit(100).build());
            for (var registro : respuesta.records()) {
                procesarRegistro(registro.data().asUtf8String());
                checkpoints.put(shard, registro.sequenceNumber());
            }
            iterator = respuesta.nextShardIterator();
            if (respuesta.records().isEmpty()) {
                return; // sin novedades: se vuelve a escanear en la próxima vuelta
            }
        }
    }

    private void procesarRegistro(String json) {
        try {
            JsonNode evento = objectMapper.readTree(json);
            String tipo = evento.path("evento").asText("");
            if ("PedidoCreado".equals(tipo)) {
                handler.procesarPedidoCreado(
                        evento.path("id").asLong(),
                        BigDecimal.valueOf(evento.path("total").asDouble()),
                        LocalDate.parse(evento.path("fecha").asText().substring(0, 10)));
                log.info("Evento del stream procesado: pedido {}", evento.path("id").asLong());
            } else if ("ProductoVisto".equals(tipo)) {
                handler.procesarProductoVisto(evento.path("id").asLong());
                log.info("Evento del stream procesado: producto {} visto", evento.path("id").asLong());
            } else {
                // Tipo desconocido: se registra completo para revisión y se salta
                log.warn("Evento de tipo '{}' no reconocido en el stream, registrado y omitido: {}",
                        tipo, json);
            }
        } catch (Exception e) {
            // Definitivo: payload ilegible. El registro sigue en el stream para
            // reproceso manual; se deja el payload completo en el log
            log.error("Evento ilegible en el stream, registrado y omitido: {} - Razón: {}",
                    json, e.getMessage());
        }
    }

    private java.util.List<String> shards() {
        var descripcion = kinesisClient.describeStream(
                DescribeStreamRequest.builder().streamName(stream).build());
        return descripcion.streamDescription().shards().stream()
                .map(s -> s.shardId())
                .toList();
    }

    private void dormirSeguro(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
