package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class LatLng implements RamaSerializable {
  public final Long latitude;
  public final Long longitude;

  public static Double distanceBetween(LatLng lla, LatLng llb) {
    // Source: https://stackoverflow.com/a/16794680/6134716
    final int R = 6371; // Radius of the earth

    var lat1 = lla.getLatitude();
    var lat2 = llb.getLatitude();
    var lon1 = lla.getLongitude();
    var lon2 = llb.getLongitude();


    double latDistance = Math.toRadians(lat2 - lat1);
    double lonDistance = Math.toRadians(lon2 - lon1);
    double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
        * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double distance = R * c * 1000; // convert to meters


    distance = Math.pow(distance, 2);

    return Math.sqrt(distance);
  }
}
