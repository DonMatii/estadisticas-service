package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// VERIFIER PROBE (scratch only): retry/DLQ contract of the statistics consumer.
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"pedidos", "pedidos.DLT"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.topic=pedidos",
        "app.kafka.reintentos.max=2",
        "app.kafka.reintentos.espera-ms=1000",
        "spring.datasource.url=jdbc:h2:mem:probe_dlq;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PedidoCreadoDlqReintentosIntegrationTest {

    private static final String TOPICO = "pedidos";
    private static final String TOPICO_DLT = "pedidos.DLT";

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private EmbeddedKafkaBroker kafkaEmbebido;
    @SpyBean private EstadisticasRepository repositorio;

    @Test @Order(1)
    void definitivoVaALaDltSinReintentosYNoSeProcesa() throws Exception {
        String payload = "{\"evento\":\"PedidoCreado\",\"total\":100,\"fecha\":\"2026-11-10T09:00:00\"}";
        long t0 = System.currentTimeMillis();
        kafkaTemplate.send(TOPICO, payload).get();

        try (Consumer<String, String> lector = crearLectorDlt()) {
            ConsumerRecord<String, String> rec = esperarPayload(lector, payload, 15_000);
            assertNotNull(rec, "evento sin id debia quedar en la DLT");
            long ms = System.currentTimeMillis() - t0;
            assertTrue(ms < 1500, "sin reintentos: la DLT llego a los " + ms + " ms (esperado < 1500)");
            String causa = rec.headers().lastHeader("kafka_dlt-exception-cause-fqcn") != null
                    ? new String(rec.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value()) : "";
            assertTrue(causa.contains("IllegalArgumentException"), "causa esperada IllegalArgumentException, fue: " + causa);
        }
        verify(repositorio, never()).procesarPedido(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test @Order(2)
    void transitorioPersistenteSeReintentaDosVecesYTerminaEnLaDlt() throws Exception {
        String payload = "{\"evento\":\"PedidoCreado\",\"id\":777,\"total\":5000,\"fecha\":\"2026-11-10T10:00:00\"}";
        doThrow(new DataAccessResourceFailureException("base no disponible"))
                .when(repositorio).procesarPedido(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());

        long t0 = System.currentTimeMillis();
        kafkaTemplate.send(TOPICO, payload).get();

        verify(repositorio, timeout(25_000).times(3)).procesarPedido(
                ArgumentMatchers.eq(777L), ArgumentMatchers.any(), ArgumentMatchers.any());

        try (Consumer<String, String> lector = crearLectorDlt()) {
            assertNotNull(esperarPayload(lector, payload, 10_000),
                    "tras agotar los reintentos el mensaje debia ir a la DLT");
            long ms = System.currentTimeMillis() - t0;
            assertTrue(ms >= 1900, "debieron pasar 2 esperas de 1000 ms antes de la DLT; paso " + ms + " ms");
        }
    }

    @Test @Order(3)
    void duplicadoNoSeCuentaDosVecesNiVaAlaDlt() throws Exception {
        String payload = "{\"evento\":\"PedidoCreado\",\"id\":999,\"total\":3000,\"fecha\":\"2026-11-10T11:00:00\"}";
        kafkaTemplate.send(TOPICO, payload).get();
        verify(repositorio, timeout(20_000)).procesarPedido(
                ArgumentMatchers.eq(999L), ArgumentMatchers.any(), ArgumentMatchers.any());

        kafkaTemplate.send(TOPICO, payload).get();
        Thread.sleep(2_000);
        verify(repositorio, times(2)).procesarPedido(
                ArgumentMatchers.eq(999L), ArgumentMatchers.any(), ArgumentMatchers.any());

        try (Consumer<String, String> lector = crearLectorDlt()) {
            assertNull(esperarPayload(lector, payload, 2_500), "un duplicado nunca va a la DLT");
        }
    }

    private Consumer<String, String> crearLectorDlt() {
        Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.consumerProps("probe-" + UUID.randomUUID(), "false", kafkaEmbebido));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<String, String> lector = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        lector.subscribe(List.of(TOPICO_DLT));
        return lector;
    }

    private ConsumerRecord<String, String> esperarPayload(Consumer<String, String> lector, String payload, long msMax) {
        long fin = System.currentTimeMillis() + msMax;
        while (System.currentTimeMillis() < fin) {
            ConsumerRecords<String, String> registros = lector.poll(Duration.ofMillis(300));
            for (ConsumerRecord<String, String> rec : registros) {
                if (payload.equals(rec.value())) {
                    return rec;
                }
            }
        }
        return null;
    }
}

