package org.example.data;

import com.rpl.rama.RamaSerializable;

public record Vehicle(String vehicleId, LatLng location,
                      String creationUUID) implements RamaSerializable {
}
