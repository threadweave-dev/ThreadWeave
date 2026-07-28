# RFC002 — Overall Architecture

**Status:** Foundational

**Version:** 1.0

---

# Purpose

This RFC defines the high-level architecture of ThreadWeave.

It identifies the core components of the platform, their responsibilities, and the boundaries between them.

It intentionally avoids implementation details, protocols, and algorithms. Those belong in dedicated RFCs.

The objective is to ensure that every future feature fits naturally into the same architecture.

---

# Architectural Principles

The architecture follows the principles established in RFC000.

In particular:

* the core is language agnostic;
* orchestration and execution are strictly separated;
* every subsystem is replaceable;
* resources are first-class citizens;
* the system is event-driven.

---

# High-Level Overview

```
                 +------------------------+
                 |     Client Runtime     |
                 |  (Python, JS, Java...) |
                 +-----------+------------+
                             |
                             |
                    Execution Protocol
                             |
                             v
+---------------------------------------------------------------+
|                     ThreadWeave Core (Rust)                    |
|                                                               |
|  +-------------+    +---------------+    +----------------+   |
|  | API Gateway | -> |   Scheduler   | -> | Resource Mgr   |   |
|  +-------------+    +---------------+    +----------------+   |
|           |                   |                    |           |
|           |                   v                    |           |
|           |            +--------------+            |           |
|           |            | Broker Layer |            |           |
|           |            +--------------+            |           |
|           |                   |                    |           |
|           |                   v                    |           |
|           |            +--------------+            |           |
|           |            | Worker Nodes |------------+           |
|           |            +--------------+                        |
|           |                   |                                |
|           |                   v                                |
|           |            Runtime Bridge                          |
|           |                   |                                |
|           +------------------>|                                |
|                               v                                |
|                     Language Runtime                           |
|                                                               |
|  Event Bus  <-----------------------------------------------> |
|                                                               |
|  Storage                                             Metrics   |
+---------------------------------------------------------------+
```

---

# Core Components

## 1. API Gateway

The API Gateway is the entry point into ThreadWeave.

It receives requests from language runtimes and external clients.

Typical responsibilities include:

* submit jobs;
* cancel executions;
* inspect state;
* query metadata;
* retrieve results.

The API Gateway performs no scheduling and executes no user code.

Its sole responsibility is translating external requests into internal commands.

---

## 2. Scheduler

The Scheduler decides **when** and **where** work should execute.

It is responsible for:

* prioritization;
* dependency resolution;
* retries;
* backoff strategies;
* placement decisions;
* fairness;
* load balancing.

The Scheduler never executes code.

It only produces scheduling decisions.

---

## 3. Resource Manager

The Resource Manager maintains the global view of available resources.

Examples include:

* CPU
* Memory
* GPU
* TPU
* FPGA
* Network bandwidth
* Software licenses
* Custom capabilities

Tasks request resources.

Workers advertise resources.

The Resource Manager matches both.

Scheduling decisions must rely on resources rather than worker identities.

---

## 4. Broker Layer

The Broker transports execution requests between components.

Its responsibilities include:

* reliable delivery;
* acknowledgement;
* queueing;
* ordering guarantees (when supported);
* back-pressure.

The broker implementation is pluggable.

Possible implementations include:

* Redis
* PostgreSQL
* NATS
* RabbitMQ
* Kafka
* in-memory transports

The Core interacts only through an abstract Broker interface.

---

## 5. Worker Node

A Worker Node is an execution host managed by ThreadWeave.

A worker:

* advertises available resources;
* receives execution requests;
* supervises runtime processes;
* reports lifecycle events;
* collects metrics.

Workers do not contain scheduling logic.

They simply execute assignments produced by the Scheduler.

---

## 6. Runtime Bridge

The Runtime Bridge connects the language-agnostic core with a specific language runtime.

Its responsibilities include:

* starting execution;
* serializing inputs;
* deserializing outputs;
* propagating exceptions;
* managing process isolation;
* reporting execution progress.

The Runtime Bridge knows the execution protocol.

It does not know the language semantics.

---

## 7. Language Runtime

A Runtime provides the developer-facing API for a language.

Examples:

* Python
* JavaScript
* Java
* Go
* Rust
* WASM

A Runtime is responsible for:

* task registration;
* task discovery;
* user-friendly APIs;
* argument validation;
* interaction with the Runtime Bridge.

The Runtime never performs orchestration.

---

## 8. Event Bus

Every important state transition generates an event.

Examples:

* JobCreated
* TaskScheduled
* ExecutionStarted
* ProgressUpdated
* ExecutionFailed
* RetryScheduled
* WorkerDisconnected
* ResourceAllocated

Events are the backbone of the system.

Subsystems communicate through events whenever possible.

---

## 9. Storage

Storage persists the platform state.

Possible stored information includes:

* job metadata;
* execution history;
* task definitions;
* retries;
* events;
* artifacts;
* scheduler state;
* cluster state.

Storage backends are replaceable.

---

## 10. Observability

Observability is a built-in capability rather than an optional extension.

The platform exposes:

* metrics;
* structured logs;
* distributed traces;
* execution history;
* event streams.

Every subsystem contributes observability data.

---

# Execution Flow

A typical execution follows these steps.

```
Client Runtime

    |

Submit Job

    |

API Gateway

    |

Scheduler

    |

Resource Manager

    |

Broker

    |

Worker

    |

Runtime Bridge

    |

Language Runtime

    |

User Code

    |

Events + Result

    |

Storage

    |

Client
```

Every transition may emit events.

Every event may be observed independently.

---

# Component Responsibilities

| Component        | Responsibility           |
| ---------------- | ------------------------ |
| API Gateway      | External entry point     |
| Scheduler        | Execution decisions      |
| Resource Manager | Resource allocation      |
| Broker           | Message transport        |
| Worker           | Execute assigned work    |
| Runtime Bridge   | Core/runtime integration |
| Language Runtime | Developer API            |
| Storage          | Persistence              |
| Event Bus        | System communication     |
| Observability    | Metrics, traces, logs    |

---

# Design Constraints

Every future component must satisfy the following constraints.

### Single Responsibility

Each component has exactly one primary responsibility.

---

### Replaceability

No implementation is mandatory.

Every subsystem must be replaceable behind a stable interface.

---

### Language Independence

The Core never depends on a programming language.

Only Runtime Bridges and Language Runtimes are language-specific.

---

### Event-Driven Communication

Whenever possible, components communicate through events rather than direct calls.

---

### Resource-Oriented Scheduling

Scheduling decisions must depend on declared resources rather than worker identities.

---

### Failure Isolation

A failure inside one runtime must never compromise the Core or unrelated executions.

---

# Future RFCs

This document intentionally leaves several topics unspecified.

Dedicated RFCs will define:

* execution protocol;
* scheduler algorithms;
* resource model;
* event model;
* storage abstraction;
* broker interface;
* runtime interface;
* artifact management;
* capability model;
* cluster membership;
* observability API.

---

# Non-Goals

This RFC does **not** define:

* the network protocol;
* internal Rust modules;
* serialization formats;
* scheduler implementation;
* broker implementation;
* database schema;
* runtime APIs.

Those concerns belong to subsequent RFCs.

---

# Summary

ThreadWeave is composed of a small number of independent components, each with a single responsibility.

The Core orchestrates execution.

Language runtimes expose developer-friendly APIs.

Workers execute user code.

Resources drive scheduling.

Events connect the entire platform.

This separation ensures scalability, replaceability, language independence, and long-term maintainability.
