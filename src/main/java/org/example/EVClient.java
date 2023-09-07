package org.example;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.cluster.ClusterManagerBase;
import org.example.data.*;

import java.util.List;
import java.util.UUID;

public class EVClient {

  private final Depot vehicleCreateDepot;
  private final Depot vehicleUpdateDepot;
  private final Depot userRegistrationDepot;
  private final Depot rideBeginDepot;
  private final Depot rideEndDepot;

  private final PState vehicles;
  private final PState users;
  private final PState userRideHistory;
  private final PState vehicleRide;

  public EVClient(ClusterManagerBase cluster) {
    String moduleName = EVModule.class.getName();

    vehicleCreateDepot = cluster.clusterDepot(moduleName, "*vehicleCreate");
    vehicleUpdateDepot = cluster.clusterDepot(moduleName, "*vehicleUpdate");
    userRegistrationDepot = cluster.clusterDepot(moduleName, "*userRegistration");
    rideBeginDepot = cluster.clusterDepot(moduleName, "*rideBegin");
    rideEndDepot = cluster.clusterDepot(moduleName, "*rideEnd");

    vehicles = cluster.clusterPState(moduleName, "$$vehicles");
    users = cluster.clusterPState(moduleName, "$$users");
    userRideHistory = cluster.clusterPState(moduleName, "$$userRideHistory");
    vehicleRide = cluster.clusterPState(moduleName, "$$vehicleRide");
  }

  // **********
  // Vehicles
  // **********


  public String createVehicle() {
    String creationUUID = UUID.randomUUID().toString();
    vehicleCreateDepot.append(new VehicleCreate(creationUUID));
    // query a pstate to get the id of the vehicle
    Vehicle vehicle = vehicles.selectOne(Path.mapVals().filterEqual(creationUUID));
    return vehicle.id;
  }

  public void updateVehicle(int battery, LatLng latLng) {
    vehicleUpdateDepot.append(new VehicleUpdate(battery, latLng));
  }

  // **********
  // Users
  // **********
  public Boolean createAccount(String email) {
    var creationUUID = UUID.randomUUID().toString();
    userRegistrationDepot.append(new UserRegistration(creationUUID, email));
    // query a pstate to determine if this invocation caused the user to be created
    User user = users.selectOne(Path.mapVals().filterEqual(creationUUID));
    return user.creationUUID.equals(creationUUID);
  }

  public List<CompletedRide> getUserRideHistory(String userId) {
    return userRideHistory.select(Path.key(userId));
  }

  // **********
  // Rides
  // **********
  public Boolean beginRide(String vehicleId, String userId, LatLng userLocation) {
    var rideId = UUID.randomUUID().toString();
    rideBeginDepot.append(new RideBegin(userId, vehicleId, userLocation, rideId));
    // query a pstate to determine if this invocation caused the ride to start
    var currentVehicleRideId = vehicleRide.selectOne(Path.key(vehicleId).key(rideId));
    return rideId.equals(currentVehicleRideId);
  }

  public void endRide(String vehicleId, String userId) {
    // We include the userId as only the user who started the ride can end it.
    rideEndDepot.append(new RideEnd(userId, vehicleId));
  }

}
