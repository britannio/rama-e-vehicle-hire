package org.example;

import clojure.lang.PersistentVector;
import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.integration.TaskGlobalContext;
import com.rpl.rama.integration.TaskGlobalObject;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import org.example.data.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

public class EVModule implements RamaModule {

  public static class ExtractVehicleId extends TopologyUtils.ExtractJavaField {
    public ExtractVehicleId() {
      super("vehicleId");
    }
  }


  public static class ExtractUserEmail extends TopologyUtils.ExtractJavaField {
    public ExtractUserEmail() {
      super("email");
    }
  }


  public static class GlobalKDTree implements TaskGlobalObject {
    private KDTree kdTree;
    // Used to support vehicles being moved.

    @Override
    public void prepareForTask(int taskId, TaskGlobalContext context) {
      kdTree = new KDTree();
    }

    @Override
    public void close() throws IOException {

    }

    public void insert(String vehicleId, LatLng location) {
      if (kdTree.search(vehicleId)) kdTree.delete(vehicleId);
      var point = new double[]{location.getLatitude(), location.getLongitude()};
      kdTree.insert(vehicleId, point);
    }

    public void delete(String vehicleId) {
      kdTree.delete(vehicleId);
    }

    public List<NearbyVehicle> nearestVehicles(LatLng location, int n) {
      var point = new double[]{location.getLatitude(), location.getLongitude()};
      var neighbours = kdTree.nearestNeighbors(point, n);
      var result = new ArrayList<NearbyVehicle>();
      for (int i = 0; i < neighbours.getNodes().size(); i++) {
        var node = neighbours.getNodes().get(i);
        var distance = neighbours.getDistances().get(i);
        result.add(new NearbyVehicle(node.label, distance));
      }
      return result;
    }
  }

  private static void declareUsersTopology(Topologies topologies) {
    StreamTopology users = topologies.stream("users");
    users.pstate("$$users",
        PState.mapSchema(
            String.class, // userId
            PState.fixedKeysSchema(
                "email", String.class,
                "creationUUID", String.class
            )
        )
    );
    users.pstate("$$emailToUserId", PState.mapSchema(
        String.class, // email
        String.class // userId
    ));

    users.source("*userRegistration").out("*arg")
        .macro(extractJavaFields("*arg", "*email", "*creationUUID"))
        .localSelect("$$emailToUserId", Path.key("*email")).out("*userId")
        // Stop if the email is already associated with a user
        .keepTrue(new Expr(Ops.IS_NULL, "*userId"))
        // Generate a userId
        .each(() -> UUID.randomUUID().toString()).out("*userId")
        // Set the emailToUserId entry
        .localTransform("$$emailToUserId", Path.key("*email").termVal("*userId"))
        .hashPartition("*userId")
        // Create the user
        .localTransform("$$users",
            Path.key("*userId")
                .multiPath(
                    Path.key("email").termVal("*email"),
                    Path.key("creationUUID").termVal("*creationUUID")
                )

        );

  }

  private static void declareVehiclesTopology(Topologies topologies) {
    StreamTopology vehicles = topologies.stream("vehicles");
    vehicles.pstate("$$vehicles", PState.mapSchema(
        String.class, // vehicleId
        PState.fixedKeysSchema(
            "battery", Integer.class,
            "location", String.class,
            "creationUUID", String.class
        )
    ));
    vehicles.pstate("$$vehicleLocationHistory",
        PState.mapSchema(
            String.class, // vehicleId
            PState.mapSchema(
                Long.class, // timestamp (ms)
                LatLng.class
            ).subindexed()
        ));
    vehicles.pstate("$$vehicleRide",
        PState.mapSchema(
            String.class, // vehicleId
            PState.fixedKeysSchema(
                "rideId", String.class,
                "riderId", String.class,
                "startLocation", LatLng.class,
                "startTimestamp", Long.class
            )
        )
    );


    vehicles.pstate("$$userInRide", PState.mapSchema(
        String.class, // userId
        Boolean.class
    ));

    vehicles.pstate("$$userRideHistory", PState.mapSchema(
        String.class, // userId
        PState.mapSchema(
            String.class, // rideId
            PState.fixedKeysSchema(
                "vehicleId", String.class,
                "startLocation", LatLng.class,
                "endLocation", LatLng.class,
                "startTimestamp", Long.class,
                "endTimestamp", Long.class,
                "route", PState.listSchema(LatLng.class)
            )
        )
    ));

    // Verify that a vehicle with this id does not exist
    // Add the vehicle to the vehicles pstate (battery = 0, location = (0,0)
    vehicles.source("*vehicleCreate").out("*arg")
        .macro(extractJavaFields("*arg", "*creationUUID", "*vehicleId"))
        .localTransform("$$vehicles",
            Path.key("*vehicleId")
                // Only performs the write if the current data is null
                .filterPred(Ops.IS_NULL)
                .multiPath(
                    Path.key("battery").termVal(0),
                    Path.key("location").termVal(new LatLng(0L, 0L)),
                    Path.key("creationUUID").termVal("*creationUUID")
                )
        );

    vehicles.source("*vehicleUpdate").out("*arg")
        .macro(extractJavaFields("*arg", "*vehicleId", "*battery", "*location"))
        .localTransform("$$vehicles",
            Path.key("*vehicleId")
                // Only update a vehicle if it exists
                .filterPred(Ops.IS_NOT_NULL)
                .multiPath(
                    Path.key("battery").termVal("*battery"),
                    Path.key("location").termVal("*location")
                )
        )
        .each(System::currentTimeMillis).out("*timestamp")
        .localTransform("$$vehicleLocationHistory",
            Path.key("*vehicleId", "*timestamp").termVal("*location")
        )
        // Update the vehicle location k-d tree
        .each((GlobalKDTree t, String id, LatLng location) -> {
          t.insert(id, location);
          return null;
        }, "*vehicleLocationTree", "*vehicleId", "*location")
    ;

    vehicles.source("*ride").out("*arg")
        .subSource("*arg",
            SubSource.create(RideBegin.class)
                .macro(extractJavaFields("*arg", "*userId", "*vehicleId", "*userLocation", "*rideId"))
                .localSelect("$$vehicles", Path.key("*vehicleId")).out("*vehicle")
                .each((Map<String, Object> v) -> v.get("battery"), "*vehicle").out("*battery")
                // Stop if the battery is below 10%
                .keepTrue(new Expr(Ops.GREATER_THAN_OR_EQUAL, "*battery", 10))
                .each((Map<String, Object> v) -> v.get("location"), "*vehicle").out("*location")
                .each(LatLng::distanceBetween, "*location", "*userLocation").out("*distance")
                // Stop if the user is over 25m away from the vehicle
                .keepTrue(new Expr(Ops.LESS_THAN_OR_EQUAL, "*distance", 25))
                .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
                // Stop if the vehicle is in a ride
                .keepTrue(new Expr(Ops.IS_NULL, "*vehicleRide"))
                .select("$$userInRide", Path.key("*userId").nullToVal(false)).out("*userInRide")
                // Stop if the user is in a ride
                .keepTrue(new Expr(Ops.NOT, "*userInRide"))
                .each(System::currentTimeMillis).out("*timestamp")
                // Create the ride
                .localTransform("$$vehicleRide",
                    Path.key("*vehicleId")
                        .multiPath(
                            Path.key("rideId").termVal("*rideId"),
                            Path.key("riderId").termVal("*userId"),
                            Path.key("startLocation").termVal("*location"),
                            Path.key("startTimestamp").termVal("*timestamp")
                        )
                )
                .hashPartition("*userId")
                .localSelect("$$userInRide", Path.key("*userId")).out("*userInRide")
                .ifTrue("*userInRide",
                    // TRUE: roll back the change to $$vehicleRide
                    Block.hashPartition("*vehicleId")
                        .localTransform("$$vehicleRide", Path.key("*vehicleId").termVal(null)),
                    // FALSE: update $$userInRide
                    Block.localTransform("$$userInRide", Path.key("*userId").termVal(true))
                ),
            SubSource.create(RideEnd.class)
                .macro(extractJavaFields("*arg", "*userId", "*vehicleId"))
                .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
                // Stop if the vehicle is not in a ride
                .keepTrue(new Expr(Ops.IS_NOT_NULL, "*vehicleRide"))
                .each((Map<String, Object> v) -> v.get("riderId"), "*vehicleRide").out("*riderId")
                .each((Map<String, Object> v) -> v.get("rideId"), "*vehicleRide").out("*rideId")
                .each((Map<String, Object> v) -> v.get("startLocation"), "*vehicleRide").out("*startLocation")
                .each((Map<String, Object> v) -> v.get("startTimestamp"), "*vehicleRide").out("*startTimestamp")
                // Stop if the rider is not the user
                .keepTrue(new Expr(Ops.EQUAL, "*riderId", "*userId"))
                // Wipe the vehicle ride
                .localTransform("$$vehicleRide", Path.key("*vehicleId").termVal(null))
                // Get the intermediate vehicle location history where timestamp > startTimestamp
                .each(System::currentTimeMillis).out("*endTimestamp")
                .localSelect("$$vehicles", Path.key("*vehicleId", "location")).out("*endLocation")
                .localSelect("$$vehicleLocationHistory",
                    Path.subselect(
                        Path
                            .key("*vehicleId")
                            .sortedMapRange("*startTimestamp", "*endTimestamp")
                            .mapVals()
                    )
                ).out("*vehicleLocationHistory")
                // Add startLocation to the beginning of the vehicle location history
                .each((List<LatLng> route, LatLng startLocation) -> {
                  var newRoute = new ArrayList<>(route);
                  newRoute.add(0, startLocation);
                  return newRoute;
                }, "*vehicleLocationHistory", "*startLocation").out("*route")
                .hashPartition("*userId")
                .localTransform("$$userInRide", Path.key("*userId").termVal(false))
                .localTransform("$$userRideHistory",
                    Path.key("*userId", "*rideId")
                        .multiPath(
                            Path.key("vehicleId").termVal("*vehicleId"),
                            Path.key("startLocation").termVal("*startLocation"),
                            Path.key("endLocation").termVal("*endLocation"),
                            Path.key("startTimestamp").termVal("*startTimestamp"),
                            Path.key("endTimestamp").termVal("*endTimestamp"),
                            Path.key("route").termVal("*route")
                        )
                )
        );


    topologies.query("nearestVehicles", "*point").out("*res")
        .allPartition()
        .each(GlobalKDTree::nearestVehicles, "*vehicleLocationTree", "*point", 50).out("*nearbyVehicles")
        .each(Ops.EXPLODE, "*nearbyVehicles").out("*nearbyVehicle")
        .each(NearbyVehicle::getVehicleId, "*nearbyVehicle").out("*vehicleId")
        .each(NearbyVehicle::getDistance, "*nearbyVehicle").out("*distance")
        .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
        // Skip if the vehicle is in a ride
        .keepTrue(new Expr(Ops.IS_NULL, "*vehicleRide"))
        .localSelect("$$vehicles", Path.key("*vehicleId")).out("*vehicle")
        .each((Map<String, Object> v) -> v.get("location"), "*vehicle").out("*location")
        .each((Map<String, Object> v) -> v.get("battery"), "*vehicle").out("*battery")
        .each(Ops.TUPLE, "*vehicleId", "*battery", "*location", "*distance").out("*vehicleTuple")

        .originPartition()
        .agg(Agg.topMonotonic(50, "*vehicleTuple")
            .idFunction(Ops.FIRST)
            .sortValFunction(Ops.LAST)
            .ascending()).out("*nearestTuples")
        .each((List<PersistentVector> topVehicles) -> topVehicles
            .stream()
            .map((v) -> new Vehicle((String) v.get(0), (Integer) v.get(1), (LatLng) v.get(2)))
            .collect(Collectors.toList()), "*nearestTuples").out("*res");
  }


  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareObject("*vehicleLocationTree", new GlobalKDTree());

    setup.declareDepot("*vehicleCreate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*vehicleUpdate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*userRegistration", Depot.hashBy(ExtractUserEmail.class));
    setup.declareDepot("*ride", Depot.hashBy(ExtractVehicleId.class));

    declareUsersTopology(topologies);
    declareVehiclesTopology(topologies);
  }
}


