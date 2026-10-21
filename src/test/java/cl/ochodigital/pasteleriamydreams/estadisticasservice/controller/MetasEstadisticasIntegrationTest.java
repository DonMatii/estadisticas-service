package cl.ochodigital.pasteleriamydreams.estadisticasservice.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// S4: la administración define y lee las metas por período (día, semana, mes) que alimenta
// la barra de avance del dashboard. Aislado en H2 propio: no toca la base real de MySQL
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"pedidos"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.topic=pedidos",
        "spring.datasource.url=jdbc:h2:mem:estadisticas_metas_test;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
class MetasEstadisticasIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void exponeYActualizaLasMetasDeLosPeriodos() throws Exception {
        // Sin metas guardadas vienen en cero: el panel sabe que aún no se fijó ninguna
        mockMvc.perform(get("/api/estadisticas"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.metas").exists())
                .andExpect(jsonPath("$.metas.dia").value(0))
                .andExpect(jsonPath("$.metas.semana").value(0))
                .andExpect(jsonPath("$.metas.mes").value(0));

        // La administradora guarda las metas de los tres períodos en curso
        mockMvc.perform(put("/api/estadisticas/metas")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"dia\":80000,\"semana\":400000,\"mes\":1500000}"))
                .andExpect(status().isOk());

        // Y el endpoint de estadísticas las devuelve guardadas
        mockMvc.perform(get("/api/estadisticas"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.metas.dia").value(80000))
                .andExpect(jsonPath("$.metas.semana").value(400000))
                .andExpect(jsonPath("$.metas.mes").value(1500000));
    }
}
