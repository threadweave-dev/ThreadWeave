# RFC001 — Domain Model & Glossary

**Status:** Draft

**Version:** 1.0

---

# Abstract

This document defines the canonical domain model of ThreadWeave.

Every term introduced here has exactly one meaning.

All future RFCs **MUST** use this vocabulary consistently. New concepts may be introduced, but existing definitions must never be reinterpreted.

The purpose of this RFC is to establish a shared language for contributors before discussing architecture or implementation.

---

# Design Principles

ThreadWeave deliberately separates four independent concerns:

* **What** should be executed.
* **When** it should be executed.
* **Where** it should be executed.
* **How** it should be executed.

Keeping these concerns independent makes the system extensible, language-agnostic, and easier to evolve.

---

# Domain Overview

```text
Namespace
    │
    └── contains
          │
          ▼
     Application
          │
          └── registers
                 ▼
               Task
                 │
                 └── creates
                        ▼
                      Job
                        │
                        └── may produce multiple
                               ▼
                          Execution
                               │
                               ├── produces
                               ▼
                             Result
                               │
                               └── references
                                      ▼
                                  Artifact(s)
```

Infrastructure:

```text
Cluster
    │
    ├── Node
    │      │
    │      ▼
    │   Worker
    │      │
    │      ▼
    │   Executor
    │
    ├── Scheduler
    ├── Broker
    ├── Resource Manager
    ├── Event Bus
    └── Storage
```

---

# Core Concepts

## Namespace

A **Namespace** is the highest logical isolation boundary within a ThreadWeave deployment.

It separates independent applications sharing the same cluster.

Typical use cases include:

* organizations;
* projects;
* environments (`development`, `staging`, `production`);
* SaaS tenants.

Everything belongs to exactly one Namespace.

Examples:

```
company-a
company-b
production
development
customer-42
```

Two Applications with the same name may coexist as long as they belong to different Namespaces.

```
production/image-service

development/image-service
```

---

## Application

An **Application** is the logical entry point exposed to developers.

It groups a coherent collection of Tasks and provides their execution configuration.

An Application is responsible for:

* registering Tasks;
* configuring serialization;
* configuring middleware/interceptors;
* defining default execution policies;
* connecting to the cluster.

Each Application belongs to exactly one Namespace.

### Python example

```python
from threadweave import Application

app = Application("documents")

@app.task
async def extract_text(file):
    ...
```

The Rust engine does not understand Python objects.

It only knows that the Task belongs to:

```
Namespace: production

Application: documents
```

---

## Task

A **Task** is the immutable definition of executable work.

A Task describes:

* its identifier;
* input schema;
* output schema;
* execution metadata;
* default resources;
* retry policy;
* timeout policy.

A Task does **not** represent an execution.

It is comparable to a function definition.

Example:

```
documents.extract_text
```

Thousands or millions of Jobs may originate from the same Task.

---

## Job

A **Job** is a request to execute a Task.

Each Job references exactly one Task.

A Job contains:

* unique identifier;
* input payload;
* execution metadata;
* priority;
* scheduling information;
* current state.

Example:

```
Run Task:

documents.extract_text

with input:

invoice.pdf
```

A Job exists even before execution begins.

---

## Execution

An **Execution** represents one execution attempt of a Job.

A Job may generate multiple Executions.

Reasons include:

* retries;
* worker crashes;
* failover;
* manual replay.

Example:

```
Job #42

Execution #1 → Worker crashed

Execution #2 → Timeout

Execution #3 → Success
```

Each Execution has its own lifecycle and telemetry.

---

## Result

A **Result** represents the outcome of a completed Execution.

It may contain:

* returned value;
* exception;
* execution metrics;
* generated artifacts;
* runtime metadata.

Results are immutable.

---

## Artifact

An **Artifact** is external data produced or consumed during execution.

Examples include:

* files;
* images;
* videos;
* machine learning models;
* datasets;
* archives;
* OCR outputs.

Artifacts may be:

* stored externally;
* managed by ThreadWeave;
* transported by an optional artifact backend.

The execution protocol transports references whenever possible rather than raw data.

---

# Execution Infrastructure

## Cluster

A **Cluster** is the logical execution platform composed of one or more Nodes.

It exposes a unified scheduling and execution environment regardless of the number of physical machines.

The Cluster is an abstraction.

Users submit Jobs to a Cluster—not to individual machines.

---

## Node

A **Node** is a physical or virtual machine participating in a Cluster.

A Node may host:

* Workers;
* Brokers;
* Storage;
* Scheduler;
* Monitoring services.

A Node is infrastructure.

---

## Worker

A **Worker** is a process that participates in task execution.

A Worker:

* registers itself;
* advertises resources;
* advertises capabilities;
* receives execution requests;
* reports events.

A Worker does **not** execute user code directly.

It delegates execution to one or more Executors.

---

## Executor

An **Executor** is responsible for executing user code.

Each Executor implements one execution runtime.

Examples:

* Python Executor
* Java Executor
* JavaScript Executor
* WASM Executor

A Worker may host multiple Executors.

Example:

```
Worker

├── Python Executor
└── WASM Executor
```

---

## Runtime

A **Runtime** is the developer-facing language integration.

It includes:

* the public API;
* decorators or annotations;
* serialization;
* packaging;
* executor implementation.

Examples:

* ThreadWeave Python
* ThreadWeave TypeScript
* ThreadWeave Java

The Runtime is installed by application developers.

The Executor is used by the engine.

These concepts must never be confused.

---

# Scheduling

## Scheduler

The **Scheduler** determines where and when Jobs execute.

It evaluates:

* priorities;
* resource availability;
* capabilities;
* placement constraints;
* affinity;
* quotas;
* scheduling strategy.

The Scheduler never executes user code.

---

## Queue

A **Queue** is a logical routing destination.

Queues express execution intent.

Examples:

```
default

gpu

high-priority

documents
```

Queues do **not** decide placement.

Scheduling remains the responsibility of the Scheduler.

---

# Resources & Capabilities

## Resource

A **Resource** is a measurable execution capacity that can be allocated.

Typical Resources include:

* CPU cores;
* Memory;
* GPU memory;
* Disk bandwidth;
* Network bandwidth;
* TPU capacity;
* FPGA slots;
* Software licenses.

Resources are quantitative.

Examples:

```yaml
resources:
  cpu: 4
  memory: 8GiB
  gpu: 1
```

Resources are consumed while the Job executes.

---

## Capability

A **Capability** describes something a Worker or Executor is able to provide.

Capabilities are qualitative.

Unlike Resources, they are not consumed.

Examples:

* python>=3.12
* cuda
* pytorch
* tensorflow
* ffmpeg
* libreoffice
* tesseract
* avx512
* arm64
* nvidia-driver=580

Example:

```yaml
capabilities:
  - cuda
  - pytorch
  - python>=3.12
```

Capabilities allow the Scheduler to match Jobs with compatible execution environments.

### Example

A task may require:

```yaml
resources:
  cpu: 8
  memory: 16GiB
  gpu: 1

capabilities:
  - cuda
  - pytorch
```

The Scheduler will only consider Workers satisfying **both** the resource requirements and the required capabilities.

---

## Resource Manager

The **Resource Manager** maintains the global view of allocatable resources.

It tracks:

* capacity;
* reservations;
* allocations;
* releases.

The Scheduler relies on the Resource Manager when making placement decisions.

---

# Communication

## Broker

The **Broker** transports messages between components.

Examples include:

* Job submission;
* scheduling requests;
* execution commands;
* acknowledgements;
* events.

The Broker is replaceable.

---

## Event

An **Event** is an immutable record describing something that happened.

Examples:

```
JobSubmitted

JobScheduled

ExecutionStarted

ExecutionCompleted

ExecutionFailed

WorkerRegistered

WorkerLost
```

Events are append-only.

They are never modified after publication.

---

## Event Bus

The **Event Bus** distributes Events across the platform.

It enables:

* observability;
* plugins;
* automation;
* auditing;
* metrics;
* tracing.

---

## Storage

Storage persists the state of ThreadWeave.

Implementations may store:

* Jobs;
* Executions;
* Results;
* Events;
* Artifact metadata;
* Scheduler metadata.

Storage is pluggable.

---

# Relationships

```text
Namespace
    └── Application
            └── Task
                    └── Job
                            └── Execution(s)
                                    └── Result
                                            └── Artifact(s)
```

```text
Cluster
    ├── Node(s)
    │      └── Worker(s)
    │              └── Executor(s)
    │
    ├── Scheduler
    ├── Resource Manager
    ├── Broker
    ├── Event Bus
    └── Storage
```

---

# Terminology Rules

The following terms are **not interchangeable**.

| Correct    | Do not use instead |
| ---------- | ------------------ |
| Task       | Function           |
| Job        | Task Instance      |
| Execution  | Run                |
| Worker     | Node               |
| Runtime    | Executor           |
| Resource   | Capability         |
| Capability | Resource           |
| Cluster    | Infrastructure     |
| Result     | Execution          |

---

# Naming Conventions

Throughout ThreadWeave documentation:

* Domain concepts use **PascalCase** (`Task`, `Execution`, `Namespace`).
* Protocol fields use **camelCase**.
* Runtime-specific APIs follow the conventions of their language.
* Events use the past tense (`ExecutionStarted`, `JobSubmitted`, `WorkerRegistered`).

---

# Open Questions

The following topics are intentionally left for future RFCs:

* Task versioning
* Application lifecycle
* Namespace security model
* Capability discovery
* Resource reservation
* Scheduling algorithms
* Artifact transport protocol
* Workflow composition

---

# Summary

RFC001 defines the canonical language of ThreadWeave.

Every future RFC builds upon these concepts.

No document should redefine or overload the terminology established here.
