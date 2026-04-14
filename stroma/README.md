# Stroma

Stroma is a broker state machine layer built on top of Keratin, responsible for managing and coordinating state transitions in a predictable way.

It is currently used as the internal core of Fibril.

## What it is

Stroma provides:

* a **state machine model** for system behavior
* separation between **state** and **side effects**
* integration with **Keratin** for durable state

The goal is to make system behavior easier to reason about, especially in the presence of failures.

## Role in the system

Within the broader stack:

* **Keratin** handles persistence
* **Stroma** defines how state evolves
* **Fibril** builds broker semantics on top

## Direction

Areas being explored:

* stricter modeling of state transitions
* recovery and replay from persisted logs
* coordination patterns for concurrent components
* clearer boundaries between logic and I/O

The design is still evolving as it is exercised through Fibril.

## Naming

Named after **stroma**, the supportive structure in biological systems that provides organization and context for functional components.

Here, it represents the structure that organizes and governs system state.

## Status

Early and closely tied to Fibril's development.

The concepts are still being refined, and the boundaries between components may shift.
