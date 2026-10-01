package cl.ochodigital.pasteleriamydreams.estadisticasservice.repository;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

// Acceso a las tablas de estadísticas de pedidos con JdbcTemplate, sin JPA (RF-10)
@Repository
public class EstadisticasRepository {

    private static final Logger log = LoggerFactory.getLogger(EstadisticasRepository.class);

    // Identificador fijo de la única fila de agregados de la tabla metricas_pedidos
    private static final int FILA_UNICA = 1;

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

    // Agregado simple que alimenta los campos pedidosTotales y montoTotalPedidos
    public record MetricasPedidos(Long pedidosTotales, BigDecimal montoTotalPedidos) {
    }
}
