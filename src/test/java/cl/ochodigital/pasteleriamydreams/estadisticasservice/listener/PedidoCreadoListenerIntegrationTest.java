package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.controller.EstadisticasController;
import cl.ochodigital.pasteleriamydreams.estadisticasservice.event.PedidoCreadoEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Verifica RF-10: los pedidos reales del topic `pedidos` se procesan de forma idempotente
// y llegan al endpoint de estadísticas; las propiedades H2/kafka solo aplican a ESTE contexto,
// el test original contextLoads sigue usando la configuración real (MySQL) sin enmascararla
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"pedidos"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.topic=pedidos",
        "spring.datasource.url=jdbc:h2:mem:estadisticas_kafka_test;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
class PedidoCreadoListenerIntegrationTest {

    private static final String TOPICO = "pedidos";
    private static final long TIMEOUT_ESPERA_MS = 30_000;

    @Autowired
    private EstadisticasController estadisticasController;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void procesaPedidosRealesDeFormaIdempotente() throws Exception {
        // 1) Sin eventos: el endpoint conserva sus campos existentes y trae los nuevos en cero
        mockMvc.perform(get("/api/estadisticas"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalProductosCatalogo").exists())
                .andExpect(jsonPath("$.categoriasActivas").exists())
                .andExpect(jsonPath("$.estadoServicio").value("OPERATIVO"))
                .andExpect(jsonPath("$.versionSistema").value("1.0.0"))
                .andExpect(jsonPath("$.pedidosTotales").value(0))
                .andExpect(jsonPath("$.montoTotalPedidos").value(0));

        // Un solo productor para todas las publicaciones: Kafka garantiza el orden entre
        // envíos del mismo productor, lo que hace determinista el escenario de duplicado
        try (KafkaProducer<String, String> productor = crearProductor()) {

            // 2) Tolerancia a veneno: un mensaje ilegible se registra y se salta sin romper al consumidor
            enviar(productor, "{mensaje ilegible");

            // 3) Un evento real crea la fila de métricas con conteo 1 y el monto del evento
            String eventoPedido1 = eventoPedido(1L, 33990);
            enviar(productor, eventoPedido1);
            esperarHasta(() -> pedidosTotales() == 1L
                            && montoTotalPedidos().compareTo(new BigDecimal("33990")) == 0,
                    "El pedido 1 debía sumarse a las métricas");

            // 4) El mismo evento publicado dos veces solo cuenta una vez; el pedido 2 actúa como
            //    marcador: al llegar después que el duplicado, prueba que el duplicado ya se omitió
            enviar(productor, eventoPedido1);
            enviar(productor, eventoPedido(2L, 15000));
            esperarHasta(() -> pedidosTotales() == 2L
                            && montoTotalPedidos().compareTo(new BigDecimal("48990")) == 0,
                    "El duplicado no debía duplicar las métricas");
        }

        // Solo los pedidos únicos quedaron registrados para la idempotencia
        List<Map<String, Object>> procesados =
                jdbcTemplate.queryForList("SELECT pedido_id FROM pedidos_procesados ORDER BY pedido_id");
        assertEquals(2, procesados.size(), "Solo los pedidos únicos deben quedar registrados");
        assertEquals(1L, ((Number) procesados.get(0).get("pedido_id")).longValue());
        assertEquals(2L, ((Number) procesados.get(1).get("pedido_id")).longValue());
    }

    // Productor de texto simple apuntado al broker embebido
    private KafkaProducer<String, String> crearProductor() {
        Map<String, Object> propiedades = KafkaTestUtils.producerProps(embeddedKafka);
        propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(propiedades);
    }

    // Publica y espera la confirmación del broker antes de continuar
    private void enviar(KafkaProducer<String, String> productor, String mensaje) throws Exception {
        productor.send(new ProducerRecord<>(TOPICO, mensaje)).get(10, TimeUnit.SECONDS);
    }

    // Construye el JSON con el mismo contrato exacto del productor de pedidos-service
    private String eventoPedido(Long id, int total) throws JsonProcessingException {
        return objectMapper.writeValueAsString(new PedidoCreadoEvent(
                "PedidoCreado",
                id,
                "Daniela Soto",
                "daniela@ejemplo.cl",
                "Torta de chocolate",
                3,
                total,
                LocalDateTime.now()));
    }

    // El endpoint devuelve los defaults (cero) si la consulta falla, así que se puede sondear
    private long pedidosTotales() {
        Object valor = estadisticasController.obtenerEstadisticas().get("pedidosTotales");
        return valor instanceof Long numero ? numero : 0L;
    }

    private BigDecimal montoTotalPedidos() {
        Object valor = estadisticasController.obtenerEstadisticas().get("montoTotalPedidos");
        return valor instanceof BigDecimal monto ? monto : BigDecimal.ZERO;
    }

    // Espera activa hasta que la condición se cumpla o se agote el tiempo máximo
    private void esperarHasta(BooleanSupplier condicion, String mensaje) throws InterruptedException {
        long limite = System.currentTimeMillis() + TIMEOUT_ESPERA_MS;
        while (System.currentTimeMillis() < limite) {
            if (condicion.getAsBoolean()) {
                return;
            }
            Thread.sleep(250);
        }
        fail(mensaje + " (se agotó la espera de " + TIMEOUT_ESPERA_MS + " ms)");
    }
}
