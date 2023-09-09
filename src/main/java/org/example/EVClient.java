package org.example;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.cluster.ClusterManagerBase;
import org.example.data.*;

import java.util.*;

public class EVClient {

  private final Depot vehicleCreateDepot;
  private final Depot vehicleUpdateDepot;
  private final Depot userRegistrationDepot;
  private final Depot rideBeginDepot;
  private final Depot rideEndDepot;

  private final PState vehicles;
  private final PState users;
  private final PState emailToUserId;
  private final PState userRideHistory;
  private final PState vehicleRide;
  private final PState userInRide;

  public EVClient(ClusterManagerBase cluster) {
    String moduleName = EVModule.class.getName();

    vehicleCreateDepot = cluster.clusterDepot(moduleName, "*vehicleCreate");
    vehicleUpdateDepot = cluster.clusterDepot(moduleName, "*vehicleUpdate");
    userRegistrationDepot = cluster.clusterDepot(moduleName, "*userRegistration");
    rideBeginDepot = cluster.clusterDepot(moduleName, "*rideBegin");
    rideEndDepot = cluster.clusterDepot(moduleName, "*rideEnd");

    vehicles = cluster.clusterPState(moduleName, "$$vehicles");
    users = cluster.clusterPState(moduleName, "$$users");
    emailToUserId = cluster.clusterPState(moduleName, "$$emailToUserId");
    userRideHistory = cluster.clusterPState(moduleName, "$$userRideHistory");
    vehicleRide = cluster.clusterPState(moduleName, "$$vehicleRide");
    userInRide = cluster.clusterPState(moduleName, "$$userInRide");
  }

  // **********
  // Vehicles
  // **********

  private String generateVehicleId() {
    var vehicleIdLength = 4;
    var alphanumerics = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var sb = new StringBuilder();
    var random = new Random();
    for (int i = 0; i < vehicleIdLength; i++) {
      var randomInt = random.nextInt(alphanumerics.length());
      sb.append(alphanumerics.charAt(randomInt));
    }
    return sb.toString();
  }

  public String createVehicle() {
    String creationUUID = UUID.randomUUID().toString();
    while (true) {
      String vehicleId = generateVehicleId();
      vehicleCreateDepot.append(new VehicleCreate(creationUUID, vehicleId));
      // TODO which pstate partition will this read from??
      Map<String, Object> vehicleMap = vehicles.selectOne(Path.key(vehicleId));
      if (vehicleMap.get("creationUUID").equals(creationUUID)) {
        return vehicleId;
      }
    }
  }

  public void updateVehicle(String vehicleId, int battery, LatLng latLng) {
    vehicleUpdateDepot.append(new VehicleUpdate(vehicleId, battery, latLng));
  }

  public List<Vehicle> getVehiclesNearLocation(LatLng latLng, int max) {
    // TODO implement this
    return null;
  }

  // **********
  // Users
  // **********

  private Boolean isValidEmail(String email) {
    return email.matches("^[^@]+@[^@]+$");
  }

  public Boolean createAccount(String email) {
    if (!isValidEmail(email)) {
      return false;
    }
    var creationUUID = UUID.randomUUID().toString();
    userRegistrationDepot.append(new UserRegistration(creationUUID, email));
    // select the user with a matching email and check if the creationUUID matches
    var userId = emailToUserId.selectOne(Path.key(email));
    var matchingCreationUUID = users.selectOne(Path.key(userId, "creationUUID"));
    return creationUUID.equals(matchingCreationUUID);
  }

  public List<CompletedRide> getUserRideHistory(String userId) {
    var history = userRideHistory.select(Path.key(userId));
    return null;
//    return history
//        .stream().map((Map m) -> Map.copyOf<String, Object>(m))
//        .map((Map m) -> new CompletedRide(
//        (String) m.get("userId"),
//        (String) m.get("rideId"),
//        (String) m.get("vehicleId"),
//        (LocalDateTime) LocalDateTime.ofEpochSecond((Long) m.get("beginTimestamp"), 0 , ZoneOffset.UTC),
//        (LocalDateTime) LocalDateTime.ofEpochSecond((Long) m.get("endTimestamp"), 0 , ZoneOffset.UTC),
//        (LatLng) m.get("beginLocation"),
//        (LatLng) m.get("endLocation")
//    )).collect(Collectors.toList());
  }

  // **********
  // Rides
  // **********
  public Boolean beginRide(String vehicleId, String userId, LatLng userLocation) {
    var rideId = UUID.randomUUID().toString();
    rideBeginDepot.append(new RideBegin(userId, vehicleId, userLocation, rideId));
    // query a pstate to determine if this invocation caused the ride to start
    var currentVehicleRideId = vehicleRide.selectOne(Path.key(vehicleId, "rideId"));
    return rideId.equals(currentVehicleRideId);
  }

  public void endRide(String vehicleId, String userId) {
    // We include the userId as only the user who started the ride can end it.
    rideEndDepot.append(new RideEnd(userId, vehicleId));
  }

}
