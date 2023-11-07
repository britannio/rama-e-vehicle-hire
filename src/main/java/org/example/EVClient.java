package org.example;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.QueryTopologyClient;
import com.rpl.rama.cluster.ClusterManagerBase;
import org.example.data.*;

import java.util.*;

public class EVClient {

  private final Depot vehicleCreateDepot;
  private final Depot vehicleUpdateDepot;
  private final Depot userRegistrationDepot;
  private final Depot rideDepot;

  private final PState vehicle;
  private final PState user;
  private final PState emailToUserId;
  private final PState userRideHistory;
  private final PState vehicleRide;

  private final QueryTopologyClient<List<Vehicle>> nearestVehiclesClient;

  public EVClient(ClusterManagerBase cluster) {
    String moduleName = EVModule.class.getName();

    vehicleCreateDepot = cluster.clusterDepot(moduleName, "*vehicleCreate");
    vehicleUpdateDepot = cluster.clusterDepot(moduleName, "*vehicleUpdate");
    userRegistrationDepot = cluster.clusterDepot(moduleName, "*userRegistration");
    rideDepot = cluster.clusterDepot(moduleName, "*ride");

    vehicle = cluster.clusterPState(moduleName, "$$vehicle");
    user = cluster.clusterPState(moduleName, "$$user");
    emailToUserId = cluster.clusterPState(moduleName, "$$emailToUserId");
    userRideHistory = cluster.clusterPState(moduleName, "$$userRideHistory");
    vehicleRide = cluster.clusterPState(moduleName, "$$vehicleRide");

    nearestVehiclesClient = cluster.clusterQuery(moduleName, "nearestVehicles");
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
      var actualUUID = vehicle.selectOne(Path.key(vehicleId, "creationUUID"));
      if (creationUUID.equals(actualUUID)) return vehicleId;
    }
  }

  public void updateVehicle(String vehicleId, int battery, LatLng latLng) {
    vehicleUpdateDepot.append(new VehicleUpdate(vehicleId, battery, latLng));
  }

  // Top 50 nearest vehicles
  public List<Vehicle> getVehiclesNearLocation(LatLng latLng) {
    return nearestVehiclesClient.invoke(latLng);
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
    var matchingCreationUUID = user.selectOne(Path.key(userId, "creationUUID"));

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
    // TODO this isn't guaranteed to work if we instantaneously end this ride before we get a chance to query
    var currentVehicleRideId = vehicleRide.selectOne(Path.key(vehicleId, "rideId"));
    if (rideId.equals(currentVehicleRideId)) return Optional.of(rideId);
    return Optional.empty();
  }

  public void endRide(String vehicleId, String userId) {
    // We include the userId as only the user who started the ride can end it.
    rideDepot.append(new RideEnd(userId, vehicleId));
  }

}
