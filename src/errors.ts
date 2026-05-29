// ФИО: Shamil Saitkhanov

export class QueueClosedError extends Error {
  constructor(message = "Queue is closed") {
    super(message);
    this.name = "QueueClosedError";
  }
}

export class TaskCanceledError extends Error {
  constructor(message = "Task was canceled") {
    super(message);
    this.name = "TaskCanceledError";
  }
}
