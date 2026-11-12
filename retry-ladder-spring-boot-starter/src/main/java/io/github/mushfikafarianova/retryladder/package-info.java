/**
 * Loss-free RabbitMQ retries that wait in the broker. Read the classes in this order.
 *
 * <p>What apps use:
 * <ul>
 *   <li>{@link io.github.mushfikafarianova.retryladder.RetryLadder}: call {@code handle} from a listener in
 *       manual ack mode. Runs the work, then acks, retries or parks the message.</li>
 *   <li>{@link io.github.mushfikafarianova.retryladder.MessageWork}: the app's own handling of one message.</li>
 *   <li>{@link io.github.mushfikafarianova.retryladder.RetryLadderProperties}: the settings under
 *       {@code retry-ladder.*}.</li>
 * </ul>
 *
 * <p>How it works inside (not public):
 * <ul>
 *   <li>{@code RetryPlans}: checks the settings at startup and works out each queue's waits (R-8).</li>
 *   <li>{@code QueuePlan}: one queue's waits, time limit and try limit (R-1).</li>
 *   <li>{@code StopRule}: retry or park, and why (R-2, R-3).</li>
 *   <li>{@code Decision}: the result: retry after a wait, or park with a reason, with the history headers (R-6).</li>
 *   <li>{@code Headers}: the names of the headers the library adds.</li>
 *   <li>{@code Topology}: the exchanges and queues it creates in RabbitMQ.</li>
 *   <li>{@code RetryLadderAutoConfiguration}: wires it all together in Spring Boot.</li>
 * </ul>
 *
 * <p>The design, with every failure case, is in {@code docs/02-design.md}.
 */
package io.github.mushfikafarianova.retryladder;
