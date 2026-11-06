package com.fleet.monitor.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.util.*;

@Profile("api")
@RestController
@RequestMapping("/api/fleet")
public class FleetController {

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getFleetStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("totalVehicles", 3);
        status.put("activeAlerts", 2);
        status.put("timestamp", new Date());
        return ResponseEntity.ok(status);
    }

    @GetMapping("/vehicle/{id}/telemetria")
    public ResponseEntity<List<String>> getTelemetria(@PathVariable String id) {
        return ResponseEntity.ok(
                Collections.singletonList("Datos de telemetria para " + id));
    }
}
