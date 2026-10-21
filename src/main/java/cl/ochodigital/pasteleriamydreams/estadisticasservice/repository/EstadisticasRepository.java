package cl.ochodigital.pasteleriamydreams.estadisticasservice.repository;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Acceso a las tablas de estadísticas de pedidos con JdbcTemplate, sin JPA (RF-10)
@Repository
public class EstadisticasRepository {

    private static final Logger log = LoggerFactory.getLogger(EstadisticasRepository.class);

    // Identificador fijo de la única fila de agregados de la tabla metricas_pedidos
    private static final int FILA_UNICA = 1;

    // Convierte una fila de ventas_diarias; la fecha se lee como LocalDate para no
    // depender del driver (MySQL y H2 devuelven tipos distintos)
    private static final RowMapper<VentaDiaria> MAPEADOR_VENTA = (rs, i) -> new VentaDiaria(
            rs.getDate("fecha").toLocalDate(),
            rs.getLong("pedidos"),
            rs.getBigDecimal("monto"));

    private final JdbcTemplate jdbcTemplate;

    public EstadisticasRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Crea las tablas al arrancar si no existen; si la base no está disponible solo avisa,
    // para que el servicio siga vivo (mismo estilo defensivo que el controlador)
    @PostConstruct
    public void crearTablasSiNoExisten() {
        try {
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS pedidos_procesados (
                        pedido_id BIGINT PRIMARY KEY,
                        procesado_en TIMESTAMP NOT NULL
                    )
                    """);
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS metricas_pedidos (
                        id INT PRIMARY KEY,
                        pedidos_totales BIGINT NOT NULL,
                        monto_total DECIMAL(12,2) NOT NULL,
                        actualizado_en TIMESTAMP NOT NULL
                    )
                    """);
            // Una fila por calendario con el detalle de ventas de ese día; solo se agrega,
            // no reemplaza ni modifica ninguna tabla existente
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS ventas_diarias (
                        fecha DATE PRIMARY KEY,
                        pedidos BIGINT NOT NULL,
                        monto DECIMAL(12,2) NOT NULL,
                        actualizado_en TIMESTAMP NOT NULL
                    )
                    """);
            log.info("Tablas de estadísticas de pedidos verificadas");
        } catch (Exception e) {
            log.warn("No se pudieron crear las tablas de estadísticas: {}", e.getMessage());
        }
    }

    // Idempotencia para la reentrega at-least-once de Kafka: primero se marca el pedido;
    // si la clave ya existía, se omite y no se vuelve a contar
    @Transactional
    public boolean procesarPedido(Long pedidoId, BigDecimal monto) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO pedidos_procesados (pedido_id, procesado_en) VALUES (?, CURRENT_TIMESTAMP)",
                    pedidoId);
        } catch (DuplicateKeyException e) {
            return false;
        }

        actualizarMetricas(monto);
        actualizarVentasDiarias(LocalDate.now(), monto);
        return true;
    }

    // Upsert de la fila única de agregados: incrementa los contadores o crea la fila con el primer pedido
    private void actualizarMetricas(BigDecimal monto) {
        int actualizadas = jdbcTemplate.update("""
                UPDATE metricas_pedidos
                SET pedidos_totales = pedidos_totales + 1,
                    monto_total = monto_total + ?,
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE id = ?
                """, monto, FILA_UNICA);

        if (actualizadas == 0) {
            try {
                jdbcTemplate.update("""
                        INSERT INTO metricas_pedidos (id, pedidos_totales, monto_total, actualizado_en)
                        VALUES (?, 1, ?, CURRENT_TIMESTAMP)
                        """, FILA_UNICA, monto);
            } catch (DuplicateKeyException e) {
                // Otra transacción creó la fila primero: se aplica el incremento igual
                jdbcTemplate.update("""
                        UPDATE metricas_pedidos
                        SET pedidos_totales = pedidos_totales + 1,
                            monto_total = monto_total + ?,
                            actualizado_en = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """, monto, FILA_UNICA);
            }
        }
    }

    // Lee la fila de agregados; sin filas devuelve ceros para la respuesta del endpoint
    public MetricasPedidos buscarMetricasPedidos() {
        return jdbcTemplate.query("""
                SELECT pedidos_totales, monto_total FROM metricas_pedidos WHERE id = ?
                """, (rs, i) -> new MetricasPedidos(
                        rs.getLong("pedidos_totales"),
                        rs.getBigDecimal("monto_total")),
                FILA_UNICA)
                .stream()
                .findFirst()
                .orElse(new MetricasPedidos(0L, BigDecimal.ZERO));
    }

    // Suma el monto del pedido a la fila de su día. La clave es la fecha, así que la misma
    // fuente (la JVM) decide qué es "hoy" al escribir y al leer: no hay desfase de huso horario
    private void actualizarVentasDiarias(LocalDate fecha, BigDecimal monto) {
        int actualizadas = jdbcTemplate.update("""
                UPDATE ventas_diarias
                SET pedidos = pedidos + 1,
                    monto = monto + ?,
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE fecha = ?
                """, monto, fecha);

        if (actualizadas == 0) {
            try {
                jdbcTemplate.update("""
                        INSERT INTO ventas_diarias (fecha, pedidos, monto, actualizado_en)
                        VALUES (?, 1, ?, CURRENT_TIMESTAMP)
                        """, fecha, monto);
            } catch (DuplicateKeyException e) {
                // Otra transacción creó la fila del día primero: se aplica el incremento igual
                jdbcTemplate.update("""
                        UPDATE ventas_diarias
                        SET pedidos = pedidos + 1,
                            monto = monto + ?,
                            actualizado_en = CURRENT_TIMESTAMP
                        WHERE fecha = ?
                        """, monto, fecha);
            }
        }
    }

    // Ventana completa de `dias` días que termina hoy, de más reciente a más antiguo,
    // con los días sin ventas en cero: la tabla del panel nunca muestra huecos
    public static List<VentaDiaria> ventasEnCeros(int dias) {
        LocalDate hoy = LocalDate.now();
        List<VentaDiaria> ventana = new ArrayList<>(dias);
        for (int i = 0; i < dias; i++) {
            LocalDate fecha = hoy.minusDays(i);
            ventana.add(new VentaDiaria(fecha, 0L, BigDecimal.ZERO));
        }
        return ventana;
    }

    // Ventas del día en curso; sin fila para hoy devuelve ceros
    public VentaDiaria buscarVentasDelDia() {
        LocalDate hoy = LocalDate.now();
        return jdbcTemplate.query("""
                SELECT fecha, pedidos, monto FROM ventas_diarias WHERE fecha = ?
                """, MAPEADOR_VENTA,
                hoy)
                .stream()
                .findFirst()
                .orElse(new VentaDiaria(hoy, 0L, BigDecimal.ZERO));
    }

    // Ventana de los últimos días puesta sobre las filas que existen en la base
    public List<VentaDiaria> buscarVentasUltimosDias(int dias) {
        Map<LocalDate, VentaDiaria> porFecha = new LinkedHashMap<>();
        for (VentaDiaria venta : ventasEnCeros(dias)) {
            porFecha.put(venta.fecha(), venta);
        }

        try {
            LocalDate desde = LocalDate.now().minusDays(dias - 1);
            jdbcTemplate.query("""
                    SELECT fecha, pedidos, monto FROM ventas_diarias
                    WHERE fecha >= ? AND fecha <= ?
                    """, MAPEADOR_VENTA,
                    desde, LocalDate.now())
                    .forEach(venta -> porFecha.put(venta.fecha(), venta));
        } catch (Exception e) {
            log.warn("No se pudieron leer las ventas diarias: {}", e.getMessage());
        }

        return new ArrayList<>(porFecha.values());
    }

    // Agregado simple que alimenta los campos pedidosTotales y montoTotalPedidos
    public record MetricasPedidos(Long pedidosTotales, BigDecimal montoTotalPedidos) {
    }

    // Una fila por calendario: cuántos pedidos hubo y cuánto se vendió ese día
    public record VentaDiaria(LocalDate fecha, Long pedidos, BigDecimal monto) {
    }
}
