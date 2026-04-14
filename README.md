# Keratin

<img src="./bannerlogo.svg" width="240em">

Keratin is a small durability layer built around a simple append-only segmented log.

It is designed to provide reliable persistence primitives that can be reused across systems, without pulling in higher-level concerns like broker semantics or coordination.

## What it is

At its core, Keratin is:

* an **append-only log**
* split into **segments**
* focused on **durability and performance**

The goal is to keep the design minimal and predictable, while still being fast enough for practical use.

## What it is not

Keratin does not attempt to be:

* a Write-Ahead Log (WAL) with complex indexing and recovery logic
* a full-fledged log-structured storage engine
* a distributed log with built-in replication and consensus
* a database
* a message broker

It is a building block, intended to sit underneath systems that need durable state. Any layer implementing more complex semantics can be built on top of it as a separate component/project/repo.

## Usage

Keratin is currently used by **Stroma** as its durability layer.

Other use cases may emerge over time as the design stabilizes and it is meant to be reusable.

## Direction

Areas that may be explored further:

* segment management and compaction
* crash recovery guarantees
* performance characteristics under load
* tooling for inspection and debugging

The focus will remain on keeping the core simple and understandable.

## Naming

Named after **keratin**, the durable material found in hair, nails, and similar structures.

The analogy is loose, but points to:

* durability
* layered growth (append-only)
* eventual shedding (compaction/cleanup)

## Status

Early and evolving. The core ideas are in place, but details and guarantees may change. Performance is considered satisfactory for intended use cases, but optimizations and improvements are likely as the design matures and more use cases are explored.
