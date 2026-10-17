# SeatSync presentation and live demo guide

## Before presenting

Start `python app.py` and open http://127.0.0.1:18780. Ensure all three nodes are online and Node 03 is coordinator. Use Reset demo data if you want empty seats. Open the accompanying PowerPoint and keep the browser ready. You can complete the demo without internet access.

## Suggested narration

“SeatSync explores what happens when multiple booking servers share a limited seat inventory. The user can enter through any live node, but the elected coordinator serializes reservations. Each node runs in a different operating-system process and keeps a separate database.”

### Normal booking

Select C4, enter a demonstration name, and keep Node 01 as entry server. Reserve the seat.

“Node 01 forwards this booking to Node 03. The coordinator enters the critical section, verifies the seat is free, and records a new snapshot. It confirms only after at least two replicas have durable copies.”

Point to the reservation receipt, equal booking counts, and the BOOKED and REPLICATE events.

### Concurrent requests

Select B3 and run the race.

“Twelve client requests start together and compete for this same seat through the available entry nodes. A barrier releases all contenders together. The coordinator serializes them. One creates the reservation and the remaining requests observe that the seat is already booked.”

Read the actual success, conflict, and error counts. If errors occur during an election, explain that result rather than claiming they were conflicts. Repeat on an available seat after the cluster stabilizes if needed.

### Failure and election

Run Test failover with three nodes online.

“This stops the actual coordinator process. The other replicas miss its heartbeat and start Bully election. Node 01 contacts higher IDs. Node 02 cannot reach Node 03, so Node 02 recovers the latest snapshot and announces itself.”

Read the measured failover time. Verify previous reserved seats remain unavailable. Make another booking through Node 01 or Node 02.

### Recovery

Restart Node 03.

“The returned node loads its durable database, fetches newer state from peers, and wins election because it has the highest ID. Recovery happens before it accepts coordinator writes.”

Wait for equal replica booking counts. Show the new coordinator and election events.

### Quorum demonstration

Optional: stop Node 01 and Node 02. The remaining node cannot confirm reservations. Restart both afterward.

“Two durable copies are required for confirmation. One surviving replica cannot meet that requirement.”

## Questions to prepare for

**Is a system on one laptop distributed?** The software uses independent processes, separate databases, and network messages with no shared booking memory. One laptop emulates multiple machines for the demonstration. It does not demonstrate resilience to laptop or network hardware failure.

**Which mutual-exclusion algorithm does it use?** The centralized approach. Nodes ask the elected coordinator, which uses a mutex for the critical section. It is not the Ricart–Agrawala algorithm or a token ring.

**Why Bully election?** Fixed node IDs provide an understandable priority rule and directly match the course. The highest reachable ID coordinates after state recovery.

**Why not call this consensus?** Election priority and quorum replication do not provide the terms, voting, log safety, and fencing rules required for consensus under arbitrary partitions. The supported model is a local process-crash demonstration.

**What happens if the reply disappears?** The reservation may exist even though the client did not receive confirmation. The client must retry with the same request ID. Recovery may keep unacknowledged reservations. The protocol does not claim exactly-once message delivery.

**Why use separate SQLite files?** Each replica owns its durable state. A common shared database would hide the replication problem the project is intended to demonstrate.

**What do the clocks show?** Lamport counters represent logical ordering of traced events and mutating messages. The wall-clock timestamps help read this same-laptop demo. Neither implements physical clock synchronization.

**What would improve the project?** Consensus and fencing for partition safety, incremental replicated logs instead of full snapshots, authentication, cancellation transactions, and experiments across real machines.

## Evidence

Export evidence after the demo. The presentation screenshots show actual UI states from a local run. Integration-test results are recorded separately in `test-results.json`, so their precise latency and failover measurements can differ from the browser demonstration.
