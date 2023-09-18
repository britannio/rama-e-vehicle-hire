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
  private final Depot rideDepot;

  private final PState vehicles;
  private final PState users;
  private final PState emailToUserId;
  private final PState userRideHistory;
  private final PState vehicleRide;

  public EVClient(ClusterManagerBase cluster) {
    String moduleName = EVModule.class.getName();

    vehicleCreateDepot = cluster.clusterDepot(moduleName, "*vehicleCreate");
    vehicleUpdateDepot = cluster.clusterDepot(moduleName, "*vehicleUpdate");
    userRegistrationDepot = cluster.clusterDepot(moduleName, "*userRegistration");
    rideDepot = cluster.clusterDepot(moduleName, "*ride");

    vehicles = cluster.clusterPState(moduleName, "$$vehicles");
    users = cluster.clusterPState(moduleName, "$$users");
    emailToUserId = cluster.clusterPState(moduleName, "$$emailToUserId");
    userRideHistory = cluster.clusterPState(moduleName, "$$userRideHistory");
    vehicleRide = cluster.clusterPState(moduleName, "$$vehicleRide");
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
    // A k-d tree or other 2d index structure could be used to maintain this
    // But based on tier.app, it appears that the client can display all vehicles in a given region
    // as long as the vehicle is not actively in a ride.
    /*
     * So we could go a level up, do some refactoring and implement regions thus making it trivial
     * to query for vehicles in a given region. Sorting by distance would not be required.
     * But the k-d tree would be an interesting test of Rama.
     *
     * Seems like tier.app supports paginated reads for a given region but it does eventually supply
     * all vehicles in a given region. It might actually be doing some location dependent work.
     * */
    return null;
  }

  // **********
  // Users
  // **********

  private Boolean isValidEmail(String email) {
    return email.matches("^[^@]+@[^@]+$");
  }

  public Optional<String> createAccount(String email) {
    if (!isValidEmail(email)) {
      return Optional.empty();
    }
    var creationUUID = UUID.randomUUID().toString();
    userRegistrationDepot.append(new UserRegistration(creationUUID, email));
    // select the user with a matching email and check if the creationUUID matches
    String userId = emailToUserId.selectOne(Path.key(email));
    var matchingCreationUUID = users.selectOne(Path.key(userId, "creationUUID"));

    if (creationUUID.equals(matchingCreationUUID)) return Optional.of(userId);

    return Optional.empty();
  }

  public List<CompletedRide> getUserRideHistory(String userId) {
    List<Map<String, Object>> results = userRideHistory.select(Path.key(userId).mapVals());
    return results.stream().map((m) -> new CompletedRide(
        userId,
        (String) m.get("rideId"),
        (String) m.get("vehicleId"),
        (Long) m.get("startTimestamp"),
        (Long) m.get("endTimestamp"),
        (LatLng) m.get("startLocation"),
        (LatLng) m.get("endLocation"),
        (List<LatLng>) m.get("route")
    )).toList();
  }

  // **********
  // Rides
  // **********
  public Optional<String> beginRide(String vehicleId, String userId, LatLng userLocation) {
    var rideId = UUID.randomUUID().toString();
    rideDepot.append(new RideBegin(userId, vehicleId, userLocation, rideId));
    // query a pstate to determine if this invocation caused the ride to start
    var currentVehicleRideId = vehicleRide.selectOne(Path.key(vehicleId, "rideId"));
    if (rideId.equals(currentVehicleRideId)) return Optional.of(rideId);
    return Optional.empty();
  }

  public void endRide(String vehicleId, String userId) {
    // We include the userId as only the user who started the ride can end it.
    rideDepot.append(new RideEnd(userId, vehicleId));
  }

}
