package cl.ochodigital.pasteleriamydreams.estadisticasservice.event;

import java.time.LocalDateTime;

// Refleja exactamente el evento que publica pedidos-service en el topic `pedidos` (RF-08 -> RF-10)
// Los nombres de los campos deben coincidir con los del productor, porque el JSON se parsea tal cual
public record PedidoCreadoEvent(
        String evento,
        Long id,
        String cliente,
        String email,
        String producto,
        Integer cantidad,
        Integer total,
        LocalDateTime fecha) {
}
