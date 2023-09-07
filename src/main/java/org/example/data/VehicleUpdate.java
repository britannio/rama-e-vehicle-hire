package org.example.data;

import com.rpl.rama.RamaSerializable;

public record VehicleUpdate(String vehicleId, Integer battery,
                            LatLng location) implements RamaSerializable {
}
