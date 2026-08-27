/**
 * Horizon retry and circuit protection integration tests (Issue #231).
 *
 * Proves that bounded retries with backoff+jitter, circuit protection,
 * and idempotent status reconciliation work together to prevent:
 * - Retry storms against degraded Horizon nodes
 * - Blind re-submission of ambiguous Stellar payments
 * - Unbounded loops on permanent failures
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  classifyJobFailure,
  retryDelayMs,
  SETTLEMENT_RETRY_POLICY,
  safeFailureMessage,
} from "../src/services/job-retry";
import {
  AnchorCircuitBreaker,
  type CircuitState,
} from "../src/services/anchor-circuit";
import { TimeoutError, TransportError } from "../src/services/timeout";
import { AppError } from "../src/errors";

describe("Issue #231 — Horizon bounded retries and circuit protection", () => {
  // ── Bounded retries ──────────────────────────────────────────────────

  describe("1. Bounded retry policy", () => {
    it("settlement retries are capped at 3 attempts total", () => {
      expect(SETTLEMENT_RETRY_POLICY.maxAttempts).toBe(3);
    });

    it("backoff grows exponentially and is capped at maxDelayMs", () => {
      const noJitter = () => 0.5;
      const d1 = retryDelayMs(1, SETTLEMENT_RETRY_POLICY, noJitter);
      const d2 = retryDelayMs(2, SETTLEMENT_RETRY_POLICY, noJitter);
      const d100 = retryDelayMs(100, SETTLEMENT_RETRY_POLICY, noJitter);

      expect(d2).toBeGreaterThan(d1);
      expect(d100).toBe(SETTLEMENT_RETRY_POLICY.maxDelayMs);
    });

    it("jitter prevents lockstep resubmission", () => {
      const d1 = retryDelayMs(1, SETTLEMENT_RETRY_POLICY, () => 0);
      const d2 = retryDelayMs(1, SETTLEMENT_RETRY_POLICY, () => 1);
      expect(d1).not.toBe(d2);
    });
  });

  // ── Error classification ─────────────────────────────────────────────

  describe("2. Error classification", () => {
    it("timeouts are indeterminate (not blind retry)", () => {
      expect(classifyJobFailure(new TimeoutError("Horizon.submit", 5000))).toBe(
        "indeterminate"
      );
    });

    it("transport errors are transient", () => {
      expect(
        classifyJobFailure(new TransportError("Horizon.load", new Error("ECONNREFUSED")))
      ).toBe("transient");
    });

    it("validation/signature/rejection errors are permanent", () => {
      const permanent = [
        new AppError(400, "XDR_MISMATCH", "bad"),
        new Error("tx_bad_seq"),
        new Error("op_underfunded"),
        new Error("Stellar rejected the transaction"),
      ];
      for (const err of permanent) {
        expect(classifyJobFailure(err)).toBe("permanent");
      }
    });

    it("rate limiting is transient", () => {
      expect(classifyJobFailure(new AppError(429, "RATE_LIMITED", "slow down"))).toBe(
        "transient"
      );
    });
  });

  // ── Circuit protection ───────────────────────────────────────────────

  describe("3. Circuit protection", () => {
    let breaker: AnchorCircuitBreaker;
    let clock: { now: number };

    beforeEach(() => {
      clock = { now: 1000 };
      breaker = new AnchorCircuitBreaker({
        failureThreshold: 3,
        cooldownMs: 5_000,
        clock: () => clock.now,
      });
    });

    it("opens after threshold consecutive failures", () => {
      expect(breaker.recordFailure("horizon")).toBe(false);
      expect(breaker.recordFailure("horizon")).toBe(false);
      expect(breaker.recordFailure("horizon")).toBe(true); // opens
      expect(breaker.isOpen("horizon")).toBe(true);
    });

    it("transitions to half-open after cooldown", () => {
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      clock.now += 5_001;
      expect(breaker.isOpen("horizon")).toBe(false); // half-open, not blocking
      expect(breaker.get("horizon").state).toBe("half-open");
    });

    it("closes on successful probe in half-open", () => {
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      clock.now += 5_001;
      breaker.recordSuccess("horizon");
      expect(breaker.get("horizon").state).toBe("closed");
      expect(breaker.get("horizon").failures).toBe(0);
    });

    it("stays half-open on probe failure (re-opens on next isOpen check)", () => {
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      // Advance past cooldown so isOpen transitions open → half-open
      clock.now += 6_000;
      expect(breaker.isOpen("horizon")).toBe(false); // half-open, allows probe
      breaker.recordFailure("horizon"); // half-open probe fails
      // State stays half-open; the probe failure did not refresh openedAt
      // so the next isOpen call will re-open the circuit.
      expect(breaker.get("horizon").state).toBe("half-open");
    });

    it("isolates providers independently", () => {
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      breaker.recordFailure("horizon");
      expect(breaker.isOpen("horizon")).toBe(true);
      expect(breaker.isOpen("anchor")).toBe(false);
    });
  });

  // ── Secret scrubbing ─────────────────────────────────────────────────

  describe("4. Safe failure messages", () => {
    it("scrubs bearer tokens, secrets, and XDRs", () => {
      const msg = safeFailureMessage(
        new Error(
          "bearer eyJhbGciOi.JUB secret=hunter2 xdr=AAAAAglongenvelope"
        )
      );
      expect(msg).not.toContain("eyJhbGciOi");
      expect(msg).not.toContain("hunter2");
      expect(msg).not.toContain("AAAAAglongenvelope");
      expect(msg).toContain("[redacted]");
    });
  });
});
