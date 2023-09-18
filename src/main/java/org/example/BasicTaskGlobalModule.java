package org.example;

import com.rpl.rama.*;
import com.rpl.rama.integration.TaskGlobalContext;
import com.rpl.rama.integration.TaskGlobalObject;
import com.rpl.rama.test.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;

import java.io.IOException;

public class BasicTaskGlobalModule implements RamaModule {

  private static class Wrapper {

    private int value = 0;

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  public static class MyGlobalInt implements TaskGlobalObject {
    private int value;

    public MyGlobalInt(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public void prepareForTask(int taskId, TaskGlobalContext context) {

    }

    @Override
    public void close() throws IOException { }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareObject("*globalValue", new MyGlobalInt(7));
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot")
        .each(MyGlobalInt::getValue, "*globalValue").out("*intVal1")
        .each(Ops.PRINTLN, "Task", new Expr(Ops.CURRENT_TASK_ID), "->", "*intVal1")
        .each((MyGlobalInt w) -> {
          w.setValue(20);
          return null;
        }, "*globalValue")
        .each(MyGlobalInt::getValue, "*globalValue").out("*intVal1")
        .each(Ops.PRINTLN, "Task", new Expr(Ops.CURRENT_TASK_ID), "->", "*intVal1")
        .shufflePartition()
        .each(MyGlobalInt::getValue, "*globalValue").out("*intVal2")
        .each(Ops.PRINTLN, "Task", new Expr(Ops.CURRENT_TASK_ID), "->", "*intVal2");
  }

  public static void main(String[] args) throws Exception {

  }
}