package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

import java.util.List;

public @Data class CompletedRide implements RamaSerializable {
  public final String userId;
  public final String rideId;
  public final String vehicleId;
  public final Long startTimestamp;
  public final Long endTimestamp;
  public final LatLng startLocation;
  public final LatLng endLocation;
  public final List<LatLng> route;
}
