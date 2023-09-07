package org.example.data;

import com.rpl.rama.RamaSerializable;

public record VehicleCreate(String creationUUID, String vehicleId) implements RamaSerializable {

}
