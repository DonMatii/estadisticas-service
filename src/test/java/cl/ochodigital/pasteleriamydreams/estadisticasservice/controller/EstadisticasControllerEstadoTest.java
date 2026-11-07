package cl.ochodigital.pasteleriamydreams.estadisticasservice.controller;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

// P0-3: el endpoint tiene que DECIR cuando está degradado. Antes devolvía ceros con
// estadoServicio="OPERATIVO" hardcodeado, que es el panel pintando $0 con total confianza
// mientras la base está caída. Un cero leído y un cero inventado no son el mismo dato.
@ExtendWith(MockitoExtension.class)
class EstadisticasControllerEstadoTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private EstadisticasRepository estadisticasRepository;

    @InjectMocks
    private EstadisticasController estadisticasController;

    @Test
    void conLaBaseSanaReportaOperativoYDatosCompletos() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class))).thenReturn(1L);
        when(estadisticasRepository.buscarMetricasPedidos())
                .thenReturn(new EstadisticasRepository.MetricasPedidos(18L, new BigDecimal("822070")));
        when(estadisticasRepository.buscarVentasDelDia())
                .thenReturn(new EstadisticasRepository.VentaDiaria(LocalDate.now(), 2L, new BigDecimal("48990")));
        when(estadisticasRepository.buscarVentasRango(any(), any()))
                .thenReturn(new EstadisticasRepository.ResumenRango(2L, new BigDecimal("48990")));
        when(estadisticasRepository.buscarMetas())
                .thenReturn(new EstadisticasRepository.Metas(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));

        Map<String, Object> stats = estadisticasController.obtenerEstadisticas();

        assertEquals("OPERATIVO", stats.get("estadoServicio"));
        assertEquals(Boolean.TRUE, stats.get("datosCompletos"));
    }

    @Test
    void conLaBaseCaidaReportaDegradadoYDatosIncompletos() {
        // Cada bloque del controller traga su propia excepción y responde con ceros: el
        // problema no es el cero, es que nunca avisó que ese cero no se leyó de la base
        RuntimeException baseCaida = new RuntimeException("base de datos no disponible");
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class))).thenThrow(baseCaida);
        when(estadisticasRepository.buscarMetricasPedidos()).thenThrow(baseCaida);
        when(estadisticasRepository.buscarVentasDelDia()).thenThrow(baseCaida);
        // El repositorio ya traga su propio error y devuelve el rango en ceros
        when(estadisticasRepository.buscarVentasRango(any(), any()))
                .thenReturn(new EstadisticasRepository.ResumenRango(0L, BigDecimal.ZERO));
        when(estadisticasRepository.buscarMetas())
                .thenReturn(new EstadisticasRepository.Metas(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));

        Map<String, Object> stats = estadisticasController.obtenerEstadisticas();

        assertEquals("DEGRADADO", stats.get("estadoServicio"));
        assertEquals(Boolean.FALSE, stats.get("datosCompletos"));
        // La estructura se conserva igual: el panel decide qué mostrar con el flag
        assertEquals(0L, stats.get("pedidosTotales"));
        assertEquals(7, ((java.util.List<?>) stats.get("ventasDiarias")).size());
    }
}
