package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// Política de errores del consumidor de estadísticas (rúbrica: éxito / error
// recuperable / error definitivo): lo definitivo va a la DLQ sin reintentos y
// sin bloquear el topic; lo transitorio se reintenta y se recupera solo.
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"pedidos", "pedidos.DLT"})
@TestPropertySource(properties = {
        "app.kinesis.habilitado=false",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.topic=pedidos",
        "app.kafka.reintentos.max=2",
        "app.kafka.reintentos.espera-ms=50",
        "spring.datasource.url=jdbc:h2:mem:estadisticas_dlq_test;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
class PedidoCreadoDlqIntegrationTest {

    private static final String TOPICO = "pedidos";
    private static final String TOPICO_DLT = "pedidos.DLT";
    private static final String EVENTO_CORRUPTO = "esto tampoco es json [[[";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbebido;

    @SpyBean
    private EstadisticasRepository repositorio;

    @Test
    void mensajeDefinitivamenteRotoVaALaDltYNoBloqueaElTopic() throws Exception {
        kafkaTemplate.send(TOPICO, EVENTO_CORRUPTO).get();

        try (Consumer<String, String> lectorDlt = crearLectorDlt()) {
            ConsumerRecord<String, String> enDlt = esperarRegistro(lectorDlt, 20_000);
            assertNotNull(enDlt, "el mensaje corrupto debía quedar aislado en la DLQ");
            assertEquals(EVENTO_CORRUPTO, enDlt.value());
        }

        // El consumidor sigue vivo: el siguiente mensaje bueno se procesa
        kafkaTemplate.send(TOPICO,
                "{\"evento\":\"PedidoCreado\",\"id\":501,\"total\":15000,\"fecha\":\"2026-11-10T11:00:00\"}")
                .get();

        verify(repositorio, timeout(20_000)).procesarPedido(
                ArgumentMatchers.eq(501L), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    void falloTransitorioSeReintentaYSeRecuperaSinIrALaDlt() throws Exception {
        // Falla TRANSITORIA: el primer procesamiento falla como si la base no
        // respondiera; el reintento entra y la venta se registra igual
        doThrow(new DataAccessResourceFailureException("base no disponible"))
                .doCallRealMethod()
                .when(repositorio).procesarPedido(ArgumentMatchers.any(), ArgumentMatchers.any(),
                        ArgumentMatchers.any());

        kafkaTemplate.send(TOPICO,
                "{\"evento\":\"PedidoCreado\",\"id\":502,\"total\":20000,\"fecha\":\"2026-11-10T12:00:00\"}")
                .get();

        // El primer intento falló y hubo al menos un reintento que procesó el pedido
        verify(repositorio, timeout(20_000).atLeast(2)).procesarPedido(
                ArgumentMatchers.eq(502L), ArgumentMatchers.any(), ArgumentMatchers.any());

        // Nada en la DLQ: la falla era recuperable
        try (Consumer<String, String> lectorDlt = crearLectorDlt()) {
            assertNull(esperarRegistro(lectorDlt, 3_000), "una falla transitoria no debía ir a la DLQ");
        }
    }

    private Consumer<String, String> crearLectorDlt() {
        Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.consumerProps("verificacion-dlt", "false", kafkaEmbebido));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<String, String> lector = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        lector.subscribe(List.of(TOPICO_DLT));
        return lector;
    }

    private ConsumerRecord<String, String> esperarRegistro(Consumer<String, String> lector, long msMax) {
        long fin = System.currentTimeMillis() + msMax;
        while (System.currentTimeMillis() < fin) {
            ConsumerRecords<String, String> registros = lector.poll(Duration.ofMillis(500));
            if (!registros.isEmpty()) {
                return registros.iterator().next();
            }
        }
        return null;
    }
}
