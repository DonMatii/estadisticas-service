package cl.ochodigital.pasteleriamydreams.estadisticasservice.listener;

import java.math.BigDecimal;
import java.time.LocalDate;

// Puerta hacia la lógica de negocio: el consumidor del stream solo traduce
// eventos, y la agregación vive en su propio componente (separación de
// responsabilidades, como en el consumidor de Kafka)
public interface EstadisticasEventosHandler {

    void procesarPedidoCreado(Long pedidoId, BigDecimal total, LocalDate fechaPedido);

    void procesarProductoVisto(Long productoId);
}
