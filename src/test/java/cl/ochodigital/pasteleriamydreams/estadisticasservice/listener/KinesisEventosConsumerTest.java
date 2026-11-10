package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

// El consumidor del stream (Entrega 3): traduce eventos del stream a la
// agregación de estadísticas. Política de errores: el evento válido se procesa,
// el ilegible o de tipo desconocido se REGISTRA completo y se salta, y ningún
// registro caótico tumba el consumidor.
class KinesisEventosConsumerTest {

    private static final String EVENTO_BUENO =
            "{\"evento\":\"PedidoCreado\",\"id\":501,\"total\":15000,\"fecha\":\"2026-11-10T11:00:00\"}";
    private static final String EVENTO_ILEGIBLE = "esto no es json {{{";
    private static final String EVENTO_DESCONOCIDO = "{\"evento\":\"AlgoNuevo\",\"id\":9}";

    private final KinesisClient kinesisClient = mock(KinesisClient.class);
    private final EstadisticasEventosHandler handler = mock(EstadisticasEventosHandler.class);

    private KinesisEventosConsumer consumer() {
        return new KinesisEventosConsumer(kinesisClient, new ObjectMapper(), handler,
                "eventos-negocio", true);
    }

    @Test
    void procesaLosEventosValidosYSaltaLosIlegiblesSinCaerse() {
        prepararShardCon(List.of(
                registro(EVENTO_BUENO),
                registro(EVENTO_ILEGIBLE),
                registro(EVENTO_DESCONOCIDO)));

        KinesisEventosConsumer consumer = consumer();
        assertDoesNotThrow(consumer::iniciar);
        try {
            // Solo el evento válido llega a la lógica de negocio
            verify(handler, timeout(10_000)).procesarPedidoCreado(
                    501L, BigDecimal.valueOf(15000.0), LocalDate.of(2026, 11, 10));
            verify(handler, timeout(10_000).times(1)).procesarPedidoCreado(any(), any(), any());
        } finally {
            consumer.detener();
        }
    }

    @Test
    void noSeRepiteLoYaProcesadoAlVolverAScanear() {
        // Primera pasada: un evento; después, vacío. El checkpoint por shard
        // evita volver a entregar lo ya procesado.
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(DescribeStreamResponse.builder()
                        .streamDescription(d -> d.streamName("eventos-negocio")
                                .shards(Shard.builder().shardId("shardId-000000000000").build()))
                        .build());
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(GetShardIteratorResponse.builder().shardIterator("iter-1").build());
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(GetRecordsResponse.builder()
                        .records(List.of(registro(EVENTO_BUENO)))
                        .nextShardIterator("iter-2")
                        .build())
                .thenReturn(GetRecordsResponse.builder()
                        .records(List.of())
                        .nextShardIterator("iter-3")
                        .build())
                .thenReturn(GetRecordsResponse.builder()
                        .records(List.of())
                        .nextShardIterator("iter-4")
                        .build());

        KinesisEventosConsumer consumer = consumer();
        consumer.iniciar();
        try {
            verify(handler, timeout(10_000)).procesarPedidoCreado(any(), any(), any());
            // Se deja correr un par de vueltas más del bucle
            Thread.sleep(1200);
            verify(handler, times(1)).procesarPedidoCreado(any(), any(), any());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.detener();
        }
    }

    private Record registro(String json) {
        return Record.builder()
                .data(SdkBytes.fromUtf8String(json))
                .sequenceNumber("seq-" + json.hashCode())
                .build();
    }

    private void prepararShardCon(List<Record> registros) {
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(DescribeStreamResponse.builder()
                        .streamDescription(d -> d.streamName("eventos-negocio")
                                .shards(Shard.builder().shardId("shardId-000000000000").build()))
                        .build());
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(GetShardIteratorResponse.builder().shardIterator("iter-1").build());
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(GetRecordsResponse.builder()
                        .records(registros)
                        .nextShardIterator("iter-2")
                        .build())
                .thenReturn(GetRecordsResponse.builder()
                        .records(List.of())
                        .nextShardIterator("iter-3")
                        .build());
    }
}
