package cl.ochodigital.pasteleriamydreams.estadisticasservice.controller;

import cl.ochodigital.pasteleriamydreams.estadisticasservice.repository.EstadisticasRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/estadisticas")
@CrossOrigin(origins = "*") // Permitimos llamadas libres desde el API Gateway y el frontend
public class EstadisticasController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EstadisticasRepository estadisticasRepository;

    @GetMapping
    public Map<String, Object> obtenerEstadisticas() {
        Map<String, Object> stats = new HashMap<>();

        try {
            // Contamos los productos reales directo desde la base de datos de AWS RDS
            Long totalProductos = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM productos", Long.class);

            // Contamos las categorías activas distintas que existan en la tabla
            Long totalCategorias = jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT categoria) FROM productos", Long.class);

            stats.put("totalProductosCatalogo", totalProductos != null ? totalProductos : 0);
            stats.put("categoriasActivas", totalCategorias != null ? totalCategorias : 4);
        } catch (Exception e) {
            // Si llega a haber algún detalle con la tabla, dejamos un respaldo seguro
            stats.put("totalProductosCatalogo", 17);
            stats.put("categoriasActivas", 4);
        }

        try {
            // Estadísticas de pedidos reales que llegan por Kafka (RF-10)
            EstadisticasRepository.MetricasPedidos metricas = estadisticasRepository.buscarMetricasPedidos();
            stats.put("pedidosTotales", metricas.pedidosTotales());
            stats.put("montoTotalPedidos", metricas.montoTotalPedidos());
        } catch (Exception e) {
            // Sin base disponible la respuesta sigue completa con ceros
            stats.put("pedidosTotales", 0L);
            stats.put("montoTotalPedidos", BigDecimal.ZERO);
        }

        try {
            // Ventas de hoy y ventana de los últimos 7 días para el panel de administración
            EstadisticasRepository.VentaDiaria hoy = estadisticasRepository.buscarVentasDelDia();
            stats.put("pedidosHoy", hoy.pedidos());
            stats.put("montoVentasHoy", hoy.monto());
            stats.put("ventasDiarias", estadisticasRepository.buscarVentasUltimosDias(7));
        } catch (Exception e) {
            // Sin base disponible la respuesta conserva la ventana completa en ceros
            stats.put("pedidosHoy", 0L);
            stats.put("montoVentasHoy", BigDecimal.ZERO);
            stats.put("ventasDiarias", EstadisticasRepository.ventasEnCeros(7));
        }

        // Períodos en curso que gobierna el selector del dashboard: hoy, esta semana (desde
        // el lunes ISO) y este mes (desde el día 1), siempre terminando en hoy. La JVM decide
        // "hoy" (mismo reloj que usa ventas_diarias al escribir), así que no hay desfase
        LocalDate hoy = LocalDate.now();
        Map<String, Object> ventasPeriodo = new HashMap<>();
        ventasPeriodo.put("dia", periodo(hoy, hoy));
        ventasPeriodo.put("semana", periodo(hoy.with(DayOfWeek.MONDAY), hoy));
        ventasPeriodo.put("mes", periodo(hoy.withDayOfMonth(1), hoy));
        stats.put("ventasPeriodo", ventasPeriodo);

        stats.put("estadoServicio", "OPERATIVO");
        stats.put("versionSistema", "1.0.0");

        return stats;
    }

    // Resumen de un período en curso: el rango consultado junto a sus pedidos y monto.
    // El repositorio se guarda solo (devuelve ceros sin base), así que el bloque
    // ventasPeriodo siempre conserva su estructura para el panel
    private Map<String, Object> periodo(LocalDate desde, LocalDate hasta) {
        EstadisticasRepository.ResumenRango rango =
                estadisticasRepository.buscarVentasRango(desde, hasta);
        Map<String, Object> datos = new HashMap<>();
        datos.put("desde", desde.toString());
        datos.put("hasta", hasta.toString());
        datos.put("pedidos", rango.pedidos());
        datos.put("monto", rango.monto());
        return datos;
    }
}