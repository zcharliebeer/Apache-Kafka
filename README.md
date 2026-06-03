# Apache Kafka Consumer Group Rebalance Optimization

This repository contains an optimized Kafka consumer implementation in Go designed to prevent temporary message processing stalls during consumer group rebalances.

## Rebalance Optimization Configurations

To ensure smooth partition handoffs and prevent "stop-the-world" stalls, the following configurations have been applied:

1. **Partition Assignment Strategy**:
   - `partition.assignment.strategy` is set to `cooperative-sticky` (equivalent to `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` in Java). This enables Incremental Cooperative Rebalancing, allowing unaffected partitions to continue processing messages during a rebalance.

2. **Consumer Stability Tuning**:
   - `max.poll.interval.ms` is set to `300000` (5 minutes) to prevent false-positive rebalances from slow processing of message batches.
   - `session.timeout.ms` is set to `45000` (45 seconds) to handle transient network hiccups without triggering immediate rebalances.
   - `heartbeat.interval.ms` is set to `3000` (3 seconds) to maintain active group membership and detect failures quickly.

3. **Non-Blocking Rebalance Listener**:
   - The rebalance callback commits offsets asynchronously in a separate goroutine during partition revocation, preventing blocking of the main consumer poll loop.
