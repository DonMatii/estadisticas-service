package cl.ochodigital.pasteleriamydreams.estadisticasservice.controller;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// "Productos más vistos" (Entrega 3): el evento producto_visto del stream de
// Kinesis incrementa el contador y el endpoint expone el top ordenado.
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"pedidos"})
@TestPropertySource(properties = {
        "app.kinesis.habilitado=false",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.topic=pedidos",
        "spring.datasource.url=jdbc:h2:mem:productos_vistos_test;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
class ProductosVistosIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EstadisticasRepository repositorio;

    @Test
    void elTopDeVistasOrdenaPorContadorDescendente() throws Exception {
        repositorio.registrarVista(5L);
        repositorio.registrarVista(5L);
        repositorio.registrarVista(9L);

        mockMvc.perform(get("/api/estadisticas/productos-vistos"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].producto_id").value(5))
                .andExpect(jsonPath("$[0].vistas").value(2))
                .andExpect(jsonPath("$[1].producto_id").value(9))
                .andExpect(jsonPath("$[1].vistas").value(1));
    }
}
