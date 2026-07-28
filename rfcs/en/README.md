# ThreadWeave RFCs

> The Request for Comments (RFC) process documents the design decisions, architecture and evolution of ThreadWeave.

RFCs are the primary source of truth for the project's architecture. Significant changes should be proposed and discussed through RFCs before implementation.

---

## Reading order

If you are discovering ThreadWeave, we recommend reading the RFCs in the following order.

### Foundations

| RFC | Title | Status |
|-----|-------|--------|
| [RFC000](./RFC000.md) | Foundational Principles | ✅ Fundamental |
| [RFC001](./RFC001.md) | Glossary & Domain Model | 🚧 Draft |
| [RFC002](./RFC002.md) | Overall Architecture | 🚧 Draft |
| [RFC003](./RFC003.md) | Extensibility Principles | 🚧 Draft |

---

## Runtime APIs

Language-specific APIs and runtime integrations.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | Python Runtime | ⏳ Planned |
| *(coming soon)* | Runtime Protocol | ⏳ Planned |
| *(coming soon)* | Capability System | ⏳ Planned |

---

## Scheduling & Resources

Scheduling, resource allocation and execution.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | Scheduler API | ⏳ Planned |
| *(coming soon)* | Resource Model | ⏳ Planned |
| *(coming soon)* | Resource Allocation | ⏳ Planned |

---

## Distributed System

Communication between components.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | Broker Interface | ⏳ Planned |
| *(coming soon)* | Event Bus | ⏳ Planned |
| *(coming soon)* | Execution Protocol | ⏳ Planned |
| *(coming soon)* | Cluster Discovery | ⏳ Planned |

---

## Storage

Persistence and recovery.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | Storage Backend API | ⏳ Planned |
| *(coming soon)* | State Recovery | ⏳ Planned |
| *(coming soon)* | Artifact Storage | ⏳ Planned |

---

## Observability

Monitoring, events and debugging.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | Event Model | ⏳ Planned |
| *(coming soon)* | Metrics | ⏳ Planned |
| *(coming soon)* | Tracing | ⏳ Planned |

---

## Governance

Project process and evolution.

| RFC | Title | Status |
|-----|-------|--------|
| *(coming soon)* | RFC Process | ⏳ Planned |
| *(coming soon)* | Compatibility Policy | ⏳ Planned |
| *(coming soon)* | Versioning Strategy | ⏳ Planned |

---

# RFC Status

| Status | Meaning |
|---------|---------|
| 📝 Proposal | Initial proposal under discussion |
| 🚧 Draft | Actively being written |
| 👀 Review | Ready for community review |
| ✅ Accepted | Official project specification |
| 🚀 Implemented | Implemented in the codebase |
| ⚠️ Superseded | Replaced by a newer RFC |
| ❌ Rejected | Proposal was rejected |

---

# RFC Naming

RFC files follow this convention:

```

RFC000.md
RFC001.md
RFC002.md
...

```

RFC numbers are never reused.

---

# Contributing

Before implementing any significant architectural change:

1. Open an issue for discussion.
2. Write or update the corresponding RFC.
3. Reach consensus.
4. Implement the change.
5. Update the RFC status.

Code follows RFCs—not the other way around.

---

# Philosophy

ThreadWeave is designed to be **protocol-first**, **language-agnostic** and **resource-oriented**.

RFCs ensure that every important architectural decision is documented, reviewed and remains understandable years later.