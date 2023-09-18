package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;
import org.example.Distance;

public @Data class LatLng implements RamaSerializable {
  public final double latitude;
  public final double longitude;

  public static double distanceBetween(LatLng lla, LatLng llb) {
    return Distance.between(new double[]{lla.latitude, lla.longitude},
        new double[]{llb.latitude, llb.longitude});
  }

}
