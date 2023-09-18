package org.example;

public class Distance {

  /**
   * Calculate the distance between two points in latitude and longitude in meters.
   *
   * @return The distance between the two points in meters.
   */
  public static double between(double[] lla, double[] llb) {
    // Source: https://stackoverflow.com/a/16794680/6134716
    final int R = 6371; // Radius of the earth

    var lat1 = lla[0];
    var lat2 = llb[0];
    var lon1 = lla[1];
    var lon2 = llb[1];


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
