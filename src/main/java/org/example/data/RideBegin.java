package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class RideBegin implements RamaSerializable {
  public final String userId;
  public final String vehicleId;
  public final LatLng userLocation;
  public final String rideId;
}
