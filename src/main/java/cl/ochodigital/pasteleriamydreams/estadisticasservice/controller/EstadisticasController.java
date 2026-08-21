package cl.ochodigital.pasteleriamydreams.estadisticasservice.controller;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/estadisticas")
@CrossOrigin(origins = "*") // Permitimos llamadas libres desde el API Gateway y el frontend
public class EstadisticasController {

    @GetMapping
    public Map<String, Object> obtenerEstadisticas() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalProductosCatalogo", 17);
        stats.put("categoriasActivas", 4);
        stats.put("estadoServicio", "OPERATIVO");
        stats.put("versionSistema", "1.0.0");
        return stats;
    }
}