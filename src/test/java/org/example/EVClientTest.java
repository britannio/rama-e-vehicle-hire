package org.example;

import com.rpl.rama.Depot;
import com.rpl.rama.Path;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;
import junit.framework.TestCase;

public class EVClientTest extends TestCase {


  public void testCreateVehicle() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var module = new EVModule();
      var moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(1,1));
      EVClient client = new EVClient(ipc);

      var vehicles = ipc.clusterPState(moduleName, "$$vehicles");

      // Creating a vehicle should put it in the $$vehicles PState
      var vehicleId = client.createVehicle();
      assertNotNull(vehicles.selectOne(Path.key(vehicleId)));

    }
  }

  public void testUpdateVehicle() {
  }

  public void testCreateAccount() {
  }

  public void testGetUserRideHistory() {
  }

  public void testBeginRide() {
  }

  public void testEndRide() {
  }
}