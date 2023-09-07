package org.example.data;

import com.rpl.rama.RamaSerializable;

public class RideBegin implements RamaSerializable {
  public final String userId;
  public final String vehicleId;
  public final LatLng userLocation;
  public final String rideId;

  public RideBegin(String userId, String vehicleId, LatLng userLocation, String rideId) {
    this.userId = userId;
    this.vehicleId = vehicleId;
    this.userLocation = userLocation;
    this.rideId = rideId;
  }

}
