package org.example.data;


import com.rpl.rama.RamaSerializable;

import java.time.LocalDateTime;

public record CompletedRide(
    String userId,
    String rideId,
    String vehicleId,
    LocalDateTime beginTimestamp,
    LocalDateTime endTimestamp,
    LatLng beginLocation,
    LatLng endLocation
//    List<LatLng> route
) implements RamaSerializable {
}
