package org.example.data;

import com.rpl.rama.RamaSerializable;

public record RideBegin(String userId, String vehicleId, LatLng userLocation,
                        String rideId) implements RamaSerializable {

}
