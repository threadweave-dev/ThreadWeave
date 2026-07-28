# RFC000 — ThreadWeave Foundational Principles

**Status:** Foundational

**Version:** 1.0

---

# Preamble

ThreadWeave is not a Python framework.

ThreadWeave is not a task library.

ThreadWeave is a language-agnostic distributed execution engine designed to reliably execute, supervise, and orchestrate workloads at any scale.

Whenever architectural trade-offs are necessary, the principles defined in this document take precedence over implementation convenience.

These principles define the identity of ThreadWeave and should guide every design decision throughout the project.

---

# 1. Language-Agnostic by Design

The ThreadWeave core must not depend on any programming language.

It never manipulates Python functions, JavaScript objects, Java classes, or any language-specific construct.

Instead, it operates exclusively on universal concepts:

* Task
* Job
* Worker
* Resource
* Event
* Result
* Executor

Language runtimes are adapters—not dependencies.

### Implication

The Rust engine must remain fully operational even when no runtime is installed.

---

# 2. The Engine Orchestrates, Runtimes Execute

Responsibilities are strictly separated.

The engine is responsible for orchestration.

The runtime is responsible for executing user code.

The engine decides:

* when execution starts;
* where it runs;
* which resources are allocated;
* retry policies;
* timeout handling;
* failure recovery.

The runtime executes the business logic—and nothing else.

This separation is absolute.

---

# 3. Business Code Must Ignore Infrastructure

Application developers should never have to write code related to:

* retries;
* brokers;
* worker management;
* clustering;
* monitoring;
* scheduling;
* distributed execution.

User code should only express **what** needs to be done.

ThreadWeave determines **how** it is executed.

---

# 4. Resources Are First-Class Citizens

Execution is driven by resources, not machines.

CPU cores, memory, GPUs, AI accelerators, software licenses, storage bandwidth, or any other execution capability are explicit scheduling resources.

Tasks request resources.

They do not request workers.

The scheduler determines where those resources are available.

---

# 5. Everything Is Observable

No significant action should be invisible.

Every state transition produces an event.

Every event can be persisted.

Every execution can be reconstructed.

Every failure includes contextual information.

Observability is not an optional feature.

It is part of the architecture itself.

---

# 6. Recovery Matters More Than Success

Failure is a normal condition in distributed systems.

Processes crash.

Machines reboot.

Networks partition.

Services become temporarily unavailable.

ThreadWeave is designed to survive failures before it is optimized for performance.

Reliability is the foundation.

---

# 7. The Protocol Is the Product

The Python API is not the product.

The JavaScript runtime is not the product.

The Rust engine is not the product.

The true product is the execution protocol that enables all components to communicate consistently.

A stable protocol allows anyone to build a new runtime without modifying the core engine.

---

# 8. Every Component Is Replaceable

Every subsystem exposes a well-defined interface.

Examples include:

* Broker
* Scheduler
* Executor
* Storage
* Resource Manager
* Event Bus

No implementation should be mandatory.

Every component should be replaceable without rewriting the rest of the system.

---

# 9. Events Drive the System

Distributed systems evolve through events.

Events are the source of truth.

Components react to events rather than relying on direct coupling.

This architecture naturally improves:

* observability;
* auditing;
* fault recovery;
* extensibility.

---

# 10. APIs Must Feel Native

Every programming language has its own conventions.

Python developers should feel like they are using a Python library.

JavaScript developers should feel like they are using a JavaScript package.

Future runtimes should embrace the idioms of their ecosystems rather than exposing Rust concepts.

The engine is universal.

The APIs are native.

---

# 11. Simplicity Is a Feature

Power should not come at the expense of clarity.

ThreadWeave should expose as few abstractions as possible.

Concepts must remain clearly separated.

Each component should have a single responsibility.

Complexity belongs inside the engine—not inside user applications.

---

# 12. Open Source by Design

ThreadWeave is designed as an open platform.

The goal is not merely to provide software.

The goal is to enable an ecosystem.

Anyone should be able to build:

* new language runtimes;
* schedulers;
* brokers;
* storage backends;
* graphical interfaces;
* observability tools;
* plugins and extensions.

Interoperability always takes precedence over vendor lock-in.

---

# What ThreadWeave Is Not

ThreadWeave is **not** a Celery clone.

ThreadWeave is **not** a Kubernetes replacement.

ThreadWeave is **not** a Python framework.

ThreadWeave is **not** a BPM workflow engine.

ThreadWeave is a resource-oriented, language-agnostic distributed execution platform.

---

# Long-Term Vision

We envision a future where teams no longer ask:

> "Which task framework does your language use?"

Instead, they ask:

> "Which ThreadWeave runtime are you using?"

Programming languages become implementation details.

ThreadWeave becomes the common execution infrastructure.
