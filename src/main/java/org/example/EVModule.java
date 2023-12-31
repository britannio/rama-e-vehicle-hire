package org.example;

import clojure.lang.MapEntry;
import clojure.lang.PersistentVector;
import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import org.example.data.*;

import java.util.*;
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

  public static Block extractMapValues(Object from, String... fieldVars) {
    Block.Impl ret = Block.create();
    for (String f : fieldVars) {
      String name;
      if (Helpers.isGeneratedVar(f)) name = Helpers.getGeneratedVarPrefix(f);
      else name = f.substring(1);
      ret = ret.each(Ops.GET, from, name).out(f);
    }
    return ret;
  }


  private static void declareTopology(Topologies topologies) {
    StreamTopology s = topologies.stream("stream");
    s.pstate("$$user",
        PState.mapSchema(
            String.class, // userId
            PState.fixedKeysSchema(
                "email", String.class,
                "creationUUID", String.class,
                "inRide", Boolean.class
            )
        )
    );

    s.pstate("$$emailToUserId", PState.mapSchema(
        String.class, // email
        String.class // userId
    ));

    s.pstate("$$vehicle", PState.mapSchema(
        String.class, // vehicleId
        PState.fixedKeysSchema(
            "battery", Integer.class,
            "location", LatLng.class,
            "creationUUID", String.class
        )
    ));

    s.pstate("$$vehicleLocationHistory", PState.mapSchema(
        String.class, // vehicleId
        PState.mapSchema(
            Long.class, // timestamp (ms)
            LatLng.class
        ).subindexed()
    ));

    s.pstate("$$vehicleRide",
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

    s.pstate("$$userRideHistory", PState.mapSchema(
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

    s.source("*userRegistration").out("*arg")
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
        .localTransform("$$user",
            Path.key("*userId")
                .multiPath(
                    Path.key("email").termVal("*email"),
                    Path.key("creationUUID").termVal("*creationUUID"),
                    Path.key("inRide").termVal(false)
                )

        );

    s.source("*vehicleCreate").out("*arg")
        .macro(extractJavaFields("*arg", "*creationUUID", "*vehicleId"))
        // Update the $$vehicle PState
        .localTransform("$$vehicle",
            Path.key("*vehicleId")
                // Only performs the write if a vehicle with this id does not exist
                .filterPred(Ops.IS_NULL)
                .multiPath(
                    Path.key("battery").termVal(0),
                    Path.key("location").termVal(new LatLng(0L, 0L)),
                    Path.key("creationUUID").termVal("*creationUUID")
                )
        );

    s.source("*vehicleUpdate").out("*arg")
        .macro(extractJavaFields("*arg", "*vehicleId", "*battery", "*location"))
        .localTransform("$$vehicle",
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
        );

    s.source("*ride").out("*arg")
        .subSource("*arg",
            SubSource.create(RideBegin.class)
                .macro(extractJavaFields("*arg", "*userId", "*vehicleId", "*userLocation", "*rideId"))
                // Stop if the vehicle does not exist
                .localSelect("$$vehicle", Path.key("*vehicleId")).out("*vehicle")
                .keepTrue(new Expr(Ops.IS_NOT_NULL, "*vehicle"))
                .macro(extractMapValues("*vehicle", "*battery", "*location"))
                // Stop if the battery is below 10%
                .keepTrue(new Expr(Ops.GREATER_THAN_OR_EQUAL, "*battery", 10))
                // Stop if the user is over 25m away from the vehicle
                .each(LatLng::distanceBetween, "*location", "*userLocation").out("*distance")
                .keepTrue(new Expr(Ops.LESS_THAN_OR_EQUAL, "*distance", 25))
                // Stop if the vehicle is in a ride
                .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
                .keepTrue(new Expr(Ops.IS_NULL, "*vehicleRide"))
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
                .localSelect("$$user", Path.key("*userId", "inRide")).out("*userInRide")
                .ifTrue("*userInRide",
                    // TRUE: roll back the change to $$vehicleRide as the user is already in a different ride
                    Block.hashPartition("*vehicleId")
                        .localTransform("$$vehicleRide", Path.key("*vehicleId").termVal(null)),
                    // FALSE: update the user's inRide property
                    Block.localTransform("$$user", Path.key("*userId", "inRide").termVal(true))
                ),
            SubSource.create(RideEnd.class)
                .macro(extractJavaFields("*arg", "*userId", "*vehicleId"))
                .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
                // Stop if the vehicle is not in a ride
                .keepTrue(new Expr(Ops.IS_NOT_NULL, "*vehicleRide"))
                .macro(extractMapValues("*vehicleRide", "*riderId", "*rideId", "*startLocation", "*startTimestamp"))
                // Stop if the rider is not the user
                .keepTrue(new Expr(Ops.EQUAL, "*riderId", "*userId"))
                // Wipe the vehicle ride
                .localTransform("$$vehicleRide", Path.key("*vehicleId").termVal(null))
                // Get the intermediate vehicle location history where timestamp > startTimestamp
                .each(System::currentTimeMillis).out("*endTimestamp")
                .localSelect("$$vehicle", Path.key("*vehicleId", "location")).out("*endLocation")
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
                .localTransform("$$user", Path.key("*userId", "inRide").termVal(false))
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
        // Fetch all vehicles
        .localSelect("$$vehicle", Path.all()).out("*vehicleEntry")
        // Get the vehicleId
        .each(MapEntry::key, "*vehicleEntry").out("*vehicleId")
        .each(MapEntry::val, "*vehicleEntry").out("*vehicle")
        // Skip if the vehicle is in a ride
        .localSelect("$$vehicleRide", Path.key("*vehicleId")).out("*vehicleRide")
        .keepTrue(new Expr(Ops.IS_NULL, "*vehicleRide"))
        // Get vehicle properties
        .macro(extractMapValues("*vehicle", "*location", "*battery"))
        // Get the distance between each vehicle and the point
        .each(LatLng::distanceBetween, "*location", "*point").out("*distance")
        // Convert the vehicle to a tuple as the aggregator expects tuples
        .each(Ops.TUPLE, "*vehicleId", "*battery", "*location", "*distance").out("*vehicleTuple")
        .originPartition()

        // Get the top 50 vehicles across all partitions
        // Two-phase aggregation automatically occurs here to minimise the amount of data sent from
        // each partition to the origin partition
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
    setup.declareDepot("*vehicleCreate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*vehicleUpdate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*userRegistration", Depot.hashBy(ExtractUserEmail.class));
    setup.declareDepot("*ride", Depot.hashBy(ExtractVehicleId.class));

    declareTopology(topologies);
  }
}


