package com.tm.kafka.connect.rest;

public class ExecutionContext {

  private String taskName;

  public void setTaskName(String name) {
    this.taskName = name;
  }

  public String getTaskName() {
    return this.taskName;
  }

  public static ExecutionContext create(String taskName) {
    ExecutionContext context = new ExecutionContext();
    context.setTaskName(taskName);
    return context;
  }
}
