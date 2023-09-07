package org.example.data;

import com.rpl.rama.RamaSerializable;

public class LatLng implements RamaSerializable {
  public final Long latitude;
  public final Long longitude;

  public LatLng(Long latitude, Long longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

}
