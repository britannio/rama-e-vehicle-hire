package org.example.data;

import com.rpl.rama.RamaSerializable;

public record RideEnd(String userId, String vehicleId) implements RamaSerializable {
}
