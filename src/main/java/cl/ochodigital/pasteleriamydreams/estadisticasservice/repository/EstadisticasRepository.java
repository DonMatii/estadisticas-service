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
            // Fila única de metas por período que la administración define para la barra
            // de avance del dashboard; solo se agrega, no toca las tablas existentes
            jdbcTemplate.execute("""
                    CREATE TABLE IF NOT EXISTS metas (
                        id INT PRIMARY KEY,
                        meta_dia DECIMAL(12,2) NOT NULL,
                        meta_semana DECIMAL(12,2) NOT NULL,
                        meta_mes DECIMAL(12,2) NOT NULL,
                        actualizado_en TIMESTAMP NOT NULL
                    )
                    """);
            log.info("Tablas de estadísticas de pedidos verificadas");
        } catch (Exception e) {
            log.warn("No se pudieron crear las tablas de estadísticas: {}", e.getMessage());
        }
    }

    // Idempotencia para la reentrega at-least-once de Kafka: primero se marca el pedido;
    // si la clave ya existía, se omite y no se vuelve a contar.
    // fechaPedido es la fecha real del pedido (evento.fecha): la venta se acumula al dia que
    // ocurrió, no al dia en que el consumidor la procesa, para que los replays no la muevan
    @Transactional
    public boolean procesarPedido(Long pedidoId, BigDecimal monto, LocalDate fechaPedido) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO pedidos_procesados (pedido_id, procesado_en) VALUES (?, CURRENT_TIMESTAMP)",
                    pedidoId);
        } catch (DuplicateKeyException e) {
            return false;
        }

        actualizarMetricas(monto);
        actualizarVentasDiarias(fechaPedido, monto);
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

    // Suma el monto del pedido a la fila de su día. La clave es la fecha real del pedido
    // (evento.fecha), así que un replay o una reentrega de Kafka no desplaza la venta al
    // día equivocado aunque el consumidor la procese más tarde
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

    // Suma de pedidos y monto de las ventas_diarias dentro de [desde, hasta]; sin filas o
    // sin base devuelve ceros, para que la respuesta del panel conserve su estructura
    public ResumenRango buscarVentasRango(LocalDate desde, LocalDate hasta) {
        try {
            return jdbcTemplate.query("""
                    SELECT COALESCE(SUM(pedidos), 0) AS pedidos, COALESCE(SUM(monto), 0) AS monto
                    FROM ventas_diarias
                    WHERE fecha >= ? AND fecha <= ?
                    """, (rs, i) -> new ResumenRango(rs.getLong("pedidos"), rs.getBigDecimal("monto")),
                    desde, hasta)
                    .stream()
                    .findFirst()
                    .orElse(new ResumenRango(0L, BigDecimal.ZERO));
        } catch (Exception e) {
            log.warn("No se pudieron leer las ventas del rango {}..{}: {}", desde, hasta, e.getMessage());
            return new ResumenRango(0L, BigDecimal.ZERO);
        }
    }

    // Lee la fila única de metas; sin fila o sin base devuelve ceros (meta sin definir)
    public Metas buscarMetas() {
        try {
            return jdbcTemplate.query("""
                    SELECT meta_dia, meta_semana, meta_mes FROM metas WHERE id = ?
                    """, (rs, i) -> new Metas(
                            rs.getBigDecimal("meta_dia"),
                            rs.getBigDecimal("meta_semana"),
                            rs.getBigDecimal("meta_mes")),
                    FILA_UNICA)
                    .stream()
                    .findFirst()
                    .orElse(new Metas(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        } catch (Exception e) {
            log.warn("No se pudieron leer las metas: {}", e.getMessage());
            return new Metas(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        }
    }

    // Guarda las metas de los tres períodos en la fila única (mismo esquema que metricas)
    @Transactional
    public void guardarMetas(BigDecimal dia, BigDecimal semana, BigDecimal mes) {
        int actualizadas = jdbcTemplate.update("""
                UPDATE metas
                SET meta_dia = ?, meta_semana = ?, meta_mes = ?, actualizado_en = CURRENT_TIMESTAMP
                WHERE id = ?
                """, dia, semana, mes, FILA_UNICA);

        if (actualizadas == 0) {
            try {
                jdbcTemplate.update("""
                        INSERT INTO metas (id, meta_dia, meta_semana, meta_mes, actualizado_en)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, FILA_UNICA, dia, semana, mes);
            } catch (DuplicateKeyException e) {
                // Otra transacción creó la fila primero: se reaplica la actualización
                jdbcTemplate.update("""
                        UPDATE metas
                        SET meta_dia = ?, meta_semana = ?, meta_mes = ?, actualizado_en = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """, dia, semana, mes, FILA_UNICA);
            }
        }
    }

    // Agregado simple que alimenta los campos pedidosTotales y montoTotalPedidos
    public record MetricasPedidos(Long pedidosTotales, BigDecimal montoTotalPedidos) {
    }

    // Una fila por calendario: cuántos pedidos hubo y cuánto se vendió ese día
    public record VentaDiaria(LocalDate fecha, Long pedidos, BigDecimal monto) {
    }

    // Agregado de un rango de fechas: pedidos y monto sumados de ese período
    public record ResumenRango(Long pedidos, BigDecimal monto) {
    }

    // Las tres metas por período que define la administración para la barra de avance
    public record Metas(BigDecimal dia, BigDecimal semana, BigDecimal mes) {
    }
}
