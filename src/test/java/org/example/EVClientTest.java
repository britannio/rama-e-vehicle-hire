package org.example;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;
import junit.framework.TestCase;
import org.example.data.LatLng;

import java.util.*;

public class EVClientTest extends TestCase {

  private static final String moduleName = EVModule.class.getName();

  private EVClient launchModule(InProcessCluster ipc) {
    var module = new EVModule();
    ipc.launchModule(module, new LaunchConfig(1, 1));
    return new EVClient(ipc);
  }


  public void testCreateVehicle() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var vehicles = ipc.clusterPState(moduleName, "$$vehicle");

      // Creating a vehicle should put it in the $$vehicle PState
      var vehicleId = client.createVehicle();
      assertNotNull(vehicles.selectOne(Path.key(vehicleId)));
    }
  }

  public void testUpdateVehicle() throws Exception {
    // Changes to the location and battery of a vehicle should be reflected in the $$vehicle PState
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var vehicles = ipc.clusterPState(moduleName, "$$vehicle");

      var vehicleId = client.createVehicle();
      var location = new LatLng(1L, 2L);
      var battery = 50;
      client.updateVehicle(vehicleId, battery, location);

      Map<String, Object> vehicleMap = vehicles.selectOne(Path.key(vehicleId));
      assertEquals(battery, vehicleMap.get("battery"));
      assertEquals(location, vehicleMap.get("location"));

      // TODO assert that $$vehicleLocationHistory is updated
    }
  }

  public void testCreateAccount() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var users = ipc.clusterPState(moduleName, "$$user");
      var emailToUserId = ipc.clusterPState(moduleName, "$$emailToUserId");

      // Creating an account should put it in the $$user PState
      var validEmail = "test@example.com";
      assertNull(emailToUserId.selectOne(Path.key(validEmail)));
      var created = client.createAccount(validEmail);
      assertTrue(created.isPresent());
      var userId = emailToUserId.selectOne(Path.key(validEmail));
      assertNotNull(userId);
      assertNotNull(users.selectOne(Path.key(userId)));

      // Creating an account with an invalid email should fail
      var invalidEmail = "test";
      assertTrue(client.createAccount(invalidEmail).isEmpty());
      assertNull(emailToUserId.selectOne(Path.key(invalidEmail)));
    }
  }

  public void testGetUserRideHistory() throws Exception {
    // Begin a ride, update the vehicle location a few times, end the ride, expect a ride history
    // entry with the correct fields

    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);

      var userId = client.createAccount("test@example.com").orElseThrow();
      var vehicleId = client.createVehicle();
      var startLocation = new LatLng(1L, 2L);
      client.updateVehicle(vehicleId, 100, startLocation);

      // Begin a ride
      client.beginRide(vehicleId, userId, startLocation).orElseThrow();
      var intermediateLocations = List.of(
          new LatLng(3L, 4L),
          new LatLng(5L, 6L),
          new LatLng(7L, 8L)
      );
      // Update the vehicle location a few times
      for (var location : intermediateLocations) {
        client.updateVehicle(vehicleId, 100, location);
      }

      // End the ride
      client.endRide(vehicleId, userId);

      // A single ride should be present
      var rideHistory = client.getUserRideHistory(userId);
      assertEquals(1, rideHistory.size());
      var ride = rideHistory.get(0);

      // The route should contain the start location and all the updates
      assertEquals(4, ride.route.size());
      assertEquals(startLocation, ride.route.get(0));
      for (int i = 0; i < intermediateLocations.size(); i++) {
        assertEquals(intermediateLocations.get(i), ride.route.get(i + 1));
      }

    }
  }


  public void testBeginRide() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var user = ipc.clusterPState(moduleName, "$$user");
      var vehicleRide = ipc.clusterPState(moduleName, "$$vehicleRide");

      // Create a user to ride the vehicle
      var userId = client.createAccount("a@example.com").orElseThrow();

      // Create a vehicle
      var vehicleId = client.createVehicle();
      var startLocation = new LatLng(1L, 2L);

      // update the vehicle location and battery
      client.updateVehicle(vehicleId, 100, startLocation);
      assertTrue(client.beginRide(vehicleId, userId, startLocation).isPresent());
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
      assertTrue(user.selectOne(Path.key(userId, "inRide")));

      // Create a new vehicle and attempt to begin a ride
      var vehicleId2 = client.createVehicle();
      client.updateVehicle(vehicleId2, 100, startLocation);
      assertFalse(client.beginRide(vehicleId2, userId, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));

      // Create a new user and attempt to begin a ride
      var userId2 = client.createAccount("b@example.com").orElseThrow();

      // Attempt to ride a vehicle in an existing ride
      assertFalse(client.beginRide(vehicleId, userId2, startLocation).isPresent());

      // Set battery too low to start
      client.updateVehicle(vehicleId2, 9, startLocation);
      assertFalse(client.beginRide(vehicleId2, userId2, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));

      // Set battery high enough to start but user too far away
      client.updateVehicle(vehicleId2, 100, new LatLng(3L, 4L));
      assertFalse(client.beginRide(vehicleId2, userId2, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));
    }
  }

  public void testEndRide() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var vehicleRide = ipc.clusterPState(moduleName, "$$vehicleRide");
      var userRideHistory = ipc.clusterPState(moduleName, "$$userRideHistory");

      // Create a user and vehicle
      var userId = client.createAccount("a@example.com").orElseThrow();
      var vehicleId = client.createVehicle();

      // Attempt to end a ride that doesn't exist (nothing to assert)
      client.endRide(vehicleId, userId);

      // begin a ride
      var startLocation = new LatLng(1L, 2L);
      client.updateVehicle(vehicleId, 100, startLocation);
      var rideId = client.beginRide(vehicleId, userId, startLocation).orElseThrow();
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));

      // Create a second user
      var userId2 = client.createAccount("b@example.com").orElseThrow();

      // Attempt to end the ride as the second user
      client.endRide(vehicleId, userId2);
      // The ride should still be in progress
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
      // The second user's ride history should be empty
      assertNull(userRideHistory.selectOne(Path.key(userId2)));

      // Update the vehicle location and battery
      var intermediateLocation = new LatLng(3L, 4L);
      client.updateVehicle(vehicleId, 90, intermediateLocation);

      // End the ride
      client.endRide(vehicleId, userId);
      // The ride should no longer be in progress
      assertNull(vehicleRide.selectOne(Path.key(vehicleId)));
      // The user's ride history should be updated
      Map<String, Object> rideHistory = userRideHistory.selectOne(Path.key(userId));
      assertTrue(rideHistory.containsKey(rideId));
      // The ride history should contain three location points
      List<LatLng> route = userRideHistory.selectOne(Path.key(userId, rideId, "route"));
      assertEquals(2, route.size());

      // Attempt to start a new ride
      client.updateVehicle(vehicleId, 100, startLocation);
      client.beginRide(vehicleId, userId2, startLocation).orElseThrow();
      // The ride should be in progress
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
    }
  }

  public void testGetVehiclesNearLocation() throws Exception {
    try (InProcessCluster cluster = InProcessCluster.create()) {
      var module = new EVModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      var client = new EVClient(cluster);


      var referenceLocation = new LatLng(1L, 2L);
      var vehicleIdSet = new HashSet<String>();
      // Create 50 vehicles at the reference location
      for (int i = 0; i < 50; i++) {
        var vehicleId = client.createVehicle();
        vehicleIdSet.add(vehicleId);
        client.updateVehicle(vehicleId, 100, referenceLocation);
      }
      // Create 100 vehicles at random locations
      for (int i = 0; i < 100; i++) {
        var vehicleId = client.createVehicle();
        vehicleIdSet.add(vehicleId);
        var random = new Random();
        var randomLocation = new LatLng(random.nextDouble(), random.nextDouble());
        client.updateVehicle(vehicleId, 100, randomLocation);
      }

      // Assert that there are 150 vehicles
      assertEquals(150, vehicleIdSet.size());

      var vehiclesNearLocation = client.getVehiclesNearLocation(referenceLocation);
      assertEquals(50, vehiclesNearLocation.size());
      // Assert that the vehicles near the reference location are the 50 vehicles created there
      for (var vehicle : vehiclesNearLocation) {
        assertEquals(referenceLocation, vehicle.getLocation());
      }

    }
  }

  // Ignore below code

  public void testGlobalObject() throws Exception {
    try (InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicTaskGlobalModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      depot.append(null);
    }

  }


  public void testBatchBlock() throws Exception {
    List data = Arrays.asList(1, 2, 3, 4);
    Block.each(Ops.PRINTLN, "Starting batch block")
        .batchBlock(
            Block
                // pre-agg
                .each(Ops.EXPLODE, data).out("*v")
                .each(Ops.PRINTLN, "Data:", "*v")
                // agg
                .agg(Agg.count()).out("*count")
                .agg(Agg.sum("*v")).out("*sum")
                // post-agg
                .each(Ops.PRINTLN, "Count:", "*count")
                .each(Ops.PRINTLN, "Sum:", "*sum"))
        .each(Ops.PRINTLN, "Finished batch block")
        .execute();
  }
}