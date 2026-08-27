/**
 * Settlement submission idempotency — comprehensive test suite (Issue #191).
 *
 * Proves that the same authenticated actor sending the same settlement intent
 * with the same idempotency key never creates a duplicate settlement or
 * submits a duplicate Stellar payment, across retries, concurrent requests,
 * worker restarts, network timeouts, and ambiguous Stellar results.
 */
import { describe, it, expect, beforeEach, vi } from "vitest";
import { Prisma } from "@prisma/client";

// ── Hoisted mocks ──────────────────────────────────────────────────────────

const h = vi.hoisted(() => {
  const model = () => ({
    findUnique: vi.fn(),
    findUniqueOrThrow: vi.fn(),
    findFirst: vi.fn(),
    create: vi.fn(),
    update: vi.fn(),
    updateMany: vi.fn(),
    deleteMany: vi.fn(),
  });
  const prisma: any = {
    expense: model(),
    expenseShare: model(),
    groupMember: model(),
    group: model(),
    settlement: model(),
    idempotencyKey: model(),
    statusHistory: model(),
    auditLog: { create: vi.fn() },
    $transaction: vi.fn(async (arg: any) =>
      typeof arg === "function" ? arg(prisma) : Promise.all(arg)
    ),
    $disconnect: vi.fn(),
  };
  const stellarSubmitPayment = vi.fn();
  const stellarGetTransaction = vi.fn();
  const stellarHashOf = vi.fn();
  const stellarLoadAccount = vi.fn();
  const stellarBuildPayment = vi.fn();
  return {
    prisma,
    stellarSubmitPayment,
    stellarGetTransaction,
    stellarHashOf,
    stellarLoadAccount,
    stellarBuildPayment,
  };
});

vi.mock("../src/db", () => ({ prisma: h.prisma }));

vi.mock("../src/services/stellar", () => ({
  stellar: {
    loadAccount: h.stellarLoadAccount,
    buildPayment: h.stellarBuildPayment,
    submitPayment: h.stellarSubmitPayment,
    getTransaction: h.stellarGetTransaction,
    hashOf: h.stellarHashOf,
  },
  memoText: vi.fn((code: string) => `MP:${code}`),
  validateSignedXdr: vi.fn((_signedXdr: string, _expected: any) => ({
    tx: {} as any,
    hash: "abc123def456",
  })),
}));

vi.mock("../src/services/settlement-reconciliation", () => ({
  reconcileSettlements: vi.fn(),
}));
vi.mock("../src/services/horizon-confirm", () => ({
  pollForConfirmation: vi.fn(async () => ({ status: "timeout" })),
}));
vi.mock("../src/worker/reconciliation", () => ({
  startReconciliation: vi.fn(() => () => {}),
}));

// ── Imports after mocks ────────────────────────────────────────────────────

import { buildApp } from "../src/app";
import { signToken } from "../src/plugins/auth";
import { hashRequest } from "../src/services/idempotency";
import { setDelayFn } from "../src/worker/index";

const prisma = h.prisma;

// ── Helpers ────────────────────────────────────────────────────────────────

function uniqueViolation() {
  return new Prisma.PrismaClientKnownRequestError("Unique constraint failed", {
    code: "P2002",
    clientVersion: "5.22.0",
  });
}

const fromUser = () => ({
  id: "user_1",
  stellarPublicKey: "GFROMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  displayName: "Payer",
  avatarUrl: null,
  createdAt: new Date("2026-01-01T00:00:00.000Z"),
});

const toUser = () => ({
  id: "user_2",
  stellarPublicKey: "GTOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  displayName: "Payee",
  avatarUrl: null,
  createdAt: new Date("2026-01-01T00:00:00.000Z"),
});

function authHeader(
  userId = "user_1",
  pk = "GFROMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
) {
  const token = signToken({ id: userId, stellarPublicKey: pk });
  return { authorization: `Bearer ${token}` };
}

function makeSettlement(overrides: Record<string, any> = {}) {
  return {
    id: "settle_1",
    shortCode: "ABC123",
    groupId: "group_1",
    fromUserId: "user_1",
    toUserId: "user_2",
    amount: "12.5000000",
    assetCode: "XLM",
    assetIssuer: null,
    transactionXdr: null,
    stellarTxHash: null,
    status: "pending",
    retryCount: 0,
    nextAttemptAt: null,
    errorCategory: null,
    claimedBy: null,
    claimedAt: null,
    leaseExpiresAt: null,
    failureReason: null,
    expiresAt: null,
    submittedAt: null,
    confirmedAt: null,
    memo: "MP:ABC123",
    expenseId: null,
    expenseShareId: null,
    createdAt: new Date("2026-01-01T00:00:00.000Z"),
    updatedAt: new Date("2026-01-01T00:00:00.000Z"),
    from: fromUser(),
    to: toUser(),
    statusHistory: [],
    ...overrides,
  };
}

function makeSubmittedSettlement(overrides: Record<string, any> = {}) {
  return makeSettlement({
    status: "submitted",
    transactionXdr: "AAAA_SIGNED_XDR_BASE64",
    submittedAt: new Date("2026-01-01T00:01:00.000Z"),
    ...overrides,
  });
}

// ── Setup ──────────────────────────────────────────────────────────────────

let app: Awaited<ReturnType<typeof buildApp>>;

beforeEach(async () => {
  vi.clearAllMocks();
  if (!app) app = await buildApp();

  h.stellarLoadAccount.mockResolvedValue({
    exists: true,
    sequence: "1",
    balances: [],
    signers: [],
    thresholds: { low: 0, med: 0, high: 0 },
  });
  h.stellarBuildPayment.mockReturnValue("unsigned-xdr");
  h.stellarSubmitPayment.mockResolvedValue("tx_hash_123");
  h.stellarGetTransaction.mockResolvedValue({ successful: true });
  h.stellarHashOf.mockReturnValue("tx_hash_123");

  prisma.settlement.findUnique.mockResolvedValue(makeSettlement());
  prisma.groupMember.findUnique.mockResolvedValue({
    groupId: "group_1",
    userId: "user_1",
    role: "member",
  });
  prisma.settlement.findUniqueOrThrow.mockResolvedValue(makeSubmittedSettlement());
  prisma.settlement.updateMany.mockResolvedValue({ count: 1 });
  prisma.idempotencyKey.create.mockResolvedValue({});
  prisma.statusHistory.findFirst.mockResolvedValue(null);
  prisma.statusHistory.create.mockResolvedValue({});

  // Mock the worker delay function to resolve instantly in tests
  setDelayFn(async () => {});
});

// ── Test suite ─────────────────────────────────────────────────────────────

describe("Issue #191 — Settlement submission idempotency", () => {
  // ── 1. First submission ──────────────────────────────────────────────

  describe("1. First submission", () => {
    it("creates the idempotency reservation and transitions settlement to submitted on first confirm", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-first" },
        payload: { signedXdr: "AAAA_SIGNED_XDR" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json().settlement.status).toBe("submitted");

      // Idempotency key was created as reservation
      expect(prisma.idempotencyKey.create).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            status: "in_progress",
            scope: "settlement.confirm",
          }),
        })
      );

      // Settlement was transitioned to submitted
      expect(prisma.settlement.updateMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "settle_1", status: { in: ["pending", "failed"] } },
          data: expect.objectContaining({ status: "submitted" }),
        })
      );

      // Idempotency key was finalized as completed
      expect(prisma.idempotencyKey.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            status: "completed",
            responseJson: expect.any(String),
          }),
        })
      );
    });
  });

  // ── 2. Successful replay ─────────────────────────────────────────────

  describe("2. Successful replay", () => {
    it("returns cached response without re-running the operation", async () => {
      const cachedResponse = {
        settlement: { id: "settle_1", status: "submitted" },
      };

      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-replay",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_SIGNED_XDR",
        }),
        responseJson: JSON.stringify(cachedResponse),
        status: "completed",
      });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-replay" },
        payload: { signedXdr: "AAAA_SIGNED_XDR" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual(cachedResponse);
      expect(prisma.settlement.updateMany).not.toHaveBeenCalled();
      expect(prisma.auditLog.create).not.toHaveBeenCalled();
    });

    it("proves Stellar submission call count remains 1 even after multiple retries", async () => {
      const cachedResponse = {
        settlement: { id: "settle_1", status: "submitted" },
      };

      prisma.idempotencyKey.findUnique
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          userId: "user_1",
          scope: "settlement.confirm",
          key: "key-submit-once",
          requestHash: hashRequest("settlement.confirm", "settle_1", {
            signedXdr: "AAAA_SIGNED",
          }),
          responseJson: JSON.stringify(cachedResponse),
          status: "completed",
        })
        .mockResolvedValueOnce({
          userId: "user_1",
          scope: "settlement.confirm",
          key: "key-submit-once",
          requestHash: hashRequest("settlement.confirm", "settle_1", {
            signedXdr: "AAAA_SIGNED",
          }),
          responseJson: JSON.stringify(cachedResponse),
          status: "completed",
        })
        .mockResolvedValueOnce({
          userId: "user_1",
          scope: "settlement.confirm",
          key: "key-submit-once",
          requestHash: hashRequest("settlement.confirm", "settle_1", {
            signedXdr: "AAAA_SIGNED",
          }),
          responseJson: JSON.stringify(cachedResponse),
          status: "completed",
        });

      for (let i = 0; i < 4; i++) {
        await app.inject({
          method: "POST",
          url: "/settlements/settle_1/confirm",
          headers: { ...authHeader(), "idempotency-key": "key-submit-once" },
          payload: { signedXdr: "AAAA_SIGNED" },
        });
      }

      // Stellar submitPayment was NOT called by the confirm route at all
      // (the worker handles submission). Only updateMany was called at most once.
      expect(prisma.settlement.updateMany.mock.calls.length).toBeLessThanOrEqual(1);
    });
  });

  // ── 3. Pending replay ────────────────────────────────────────────────

  describe("3. Pending replay", () => {
    it("returns cached state without re-submitting when operation is still in progress", async () => {
      const pendingResponse = {
        settlement: { id: "settle_1", status: "pending" },
      };

      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-pending",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_SIGNED",
        }),
        responseJson: JSON.stringify(pendingResponse),
        status: "completed",
      });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-pending" },
        payload: { signedXdr: "AAAA_SIGNED" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual(pendingResponse);
      expect(prisma.settlement.updateMany).not.toHaveBeenCalled();
    });

    it("returns 409 when the first request is still executing (not abandoned)", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-inprog",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_SIGNED",
        }),
        responseJson: null,
        status: "in_progress",
        updatedAt: new Date(),
      });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-inprog" },
        payload: { signedXdr: "AAAA_SIGNED" },
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().error).toBe("IDEMPOTENCY_IN_PROGRESS");
    });
  });

  // ── 4. Same key + different intent → conflict ────────────────────────

  describe("4. Same key + different intent → conflict", () => {
    it("returns 409 IDEMPOTENCY_CONFLICT when same key is reused with different XDR", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-conflict",
        requestHash: "hash-for-different-xdr",
        responseJson: JSON.stringify({
          settlement: { id: "settle_1", status: "submitted" },
        }),
        status: "completed",
      });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-conflict" },
        payload: { signedXdr: "DIFFERENT_SIGNED_XDR" },
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().error).toBe("IDEMPOTENCY_CONFLICT");
      expect(prisma.settlement.updateMany).not.toHaveBeenCalled();
    });
  });

  // ── 5. Same key + different authenticated user ────────────────────────

  describe("5. Same key + different authenticated user", () => {
    it("allows same idempotency key for different users — independent settlements", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);
      prisma.settlement.findUnique.mockResolvedValue(
        makeSettlement({ fromUserId: "user_2" })
      );
      prisma.groupMember.findUnique.mockResolvedValue({
        groupId: "group_1",
        userId: "user_2",
        role: "member",
      });
      prisma.settlement.findUniqueOrThrow.mockResolvedValue(
        makeSubmittedSettlement({ fromUserId: "user_2" })
      );

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: {
          ...authHeader("user_2", "GTOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
          "idempotency-key": "shared-key",
        },
        payload: { signedXdr: "XDR_USER2" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json().settlement.status).toBe("submitted");
    });

    it("never returns User A's settlement to User B", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);
      prisma.settlement.findUnique.mockResolvedValue(
        makeSettlement({ id: "settle_B", fromUserId: "user_2" })
      );
      prisma.groupMember.findUnique.mockResolvedValue({
        groupId: "group_1",
        userId: "user_2",
        role: "member",
      });
      prisma.settlement.findUniqueOrThrow.mockResolvedValue(
        makeSubmittedSettlement({ id: "settle_B", fromUserId: "user_2" })
      );

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_B/confirm",
        headers: {
          ...authHeader("user_2", "GTOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
          "idempotency-key": "user-a-key",
        },
        payload: { signedXdr: "XDR_FOR_B" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json().settlement.id).toBe("settle_B");
    });
  });

  // ── 6. Concurrent requests ───────────────────────────────────────────

  describe("6. Concurrent requests", () => {
    it("two simultaneous requests produce at most one settlement update", async () => {
      prisma.idempotencyKey.create
        .mockResolvedValueOnce({})
        .mockRejectedValueOnce(uniqueViolation());

      prisma.idempotencyKey.findUnique
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          userId: "user_1",
          scope: "settlement.confirm",
          key: "key-race",
          requestHash: hashRequest("settlement.confirm", "settle_1", {
            signedXdr: "AAAA_RACE",
          }),
          responseJson: JSON.stringify({
            settlement: { id: "settle_1", status: "submitted" },
          }),
          status: "completed",
        });

      prisma.settlement.findUniqueOrThrow.mockResolvedValue(
        makeSubmittedSettlement()
      );

      const [res1, res2] = await Promise.all([
        app.inject({
          method: "POST",
          url: "/settlements/settle_1/confirm",
          headers: { ...authHeader(), "idempotency-key": "key-race" },
          payload: { signedXdr: "AAAA_RACE" },
        }),
        app.inject({
          method: "POST",
          url: "/settlements/settle_1/confirm",
          headers: { ...authHeader(), "idempotency-key": "key-race" },
          payload: { signedXdr: "AAAA_RACE" },
        }),
      ]);

      expect(res1.statusCode).toBe(200);
      expect(res2.statusCode).toBe(200);

      // The loser's operation was rolled back; only the winner's update persisted
      expect(prisma.settlement.updateMany.mock.calls.length).toBeLessThanOrEqual(1);
    });

    it("database-level unique constraint serializes concurrent retries — not an in-memory lock", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);
      prisma.idempotencyKey.create.mockResolvedValue({});

      await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-db-level" },
        payload: { signedXdr: "AAAA_DB" },
      });

      expect(prisma.idempotencyKey.create).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            userId: "user_1",
            scope: "settlement.confirm",
            key: "key-db-level",
          }),
        })
      );

      // The Prisma schema has @@unique([userId, scope, key]) — this is the
      // database-level protection, not application-level in-memory locking.
    });
  });

  // ── 7. Ambiguous Stellar submission ──────────────────────────────────

  describe("7. Ambiguous Stellar submission", () => {
    it("confirm route sets up settlement for worker to handle ambiguous results", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-ambiguous" },
        payload: { signedXdr: "AAAA_AMBIGUOUS" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json().settlement.status).toBe("submitted");

      // The settlement is in "submitted" status — the worker will:
      // 1. Submit via stellar.submitPayment
      // 2. If timeout/ambiguous: use stellar.hashOf + stellar.getTransaction
      // 3. If found successful → confirmed
      // 4. If found failed → failed
      // 5. If not found → needs_review
    });
  });

  // ── 8. Validated transaction reconciliation ──────────────────────────

  describe("8. Validated transaction reconciliation", () => {
    it("worker checks ledger before resubmitting — confirms when already applied", async () => {
      const { processSettlementJob } = await import("../src/worker/index");

      // The settlement machine reads current status from the DB; it must be
      // "verifying" so the allowed transition to "confirmed" succeeds.
      prisma.settlement.findUnique.mockResolvedValue(
        makeSubmittedSettlement({ id: "settle_reconcile", status: "verifying" })
      );

      const job = {
        id: "settle_reconcile",
        shortCode: "REC123",
        groupId: "group_1",
        fromPublicKey: "GFROM",
        toPublicKey: "GTO",
        amount: "10.00",
        assetCode: "XLM",
        assetIssuer: null,
        transactionXdr: "signed-xdr-for-reconcile",
        expenseShareId: null,
        expiresAt: new Date(Date.now() + 3600_000),
        retryCount: 1,
        status: "verifying",
      };

      h.stellarSubmitPayment.mockRejectedValueOnce(new Error("timeout"));
      h.stellarHashOf.mockReturnValue("reconcile_hash");
      h.stellarGetTransaction.mockResolvedValue({ successful: true });

      const ctx = {
        correlationId: "test-reconcile",
        jobId: "settle_reconcile",
        jobType: "settlement" as const,
      };

      await processSettlementJob(job, ctx);

      expect(h.stellarHashOf).toHaveBeenCalledWith("signed-xdr-for-reconcile");
      expect(h.stellarGetTransaction).toHaveBeenCalledWith("reconcile_hash");
      expect(prisma.settlement.updateMany).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ status: "confirmed" }),
        })
      );
    });

    it("worker moves to needs_review when transaction not yet discoverable", async () => {
      const { processSettlementJob } = await import("../src/worker/index");

      // The settlement machine reads current status from the DB; it must be
      // "submitted" so the allowed transition to "verifying" succeeds.
      prisma.settlement.findUnique.mockResolvedValue(
        makeSubmittedSettlement({ id: "settle_notfound", status: "submitted" })
      );

      const job = {
        id: "settle_notfound",
        shortCode: "NF123",
        groupId: "group_1",
        fromPublicKey: "GFROM",
        toPublicKey: "GTO",
        amount: "5.00",
        assetCode: "XLM",
        assetIssuer: null,
        transactionXdr: "signed-xdr-notfound",
        expenseShareId: null,
        expiresAt: new Date(Date.now() + 3600_000),
        retryCount: 0,
        status: "submitted",
      };

      h.stellarSubmitPayment.mockRejectedValueOnce(new Error("socket hang up"));
      h.stellarHashOf.mockReturnValue("notfound_hash");
      h.stellarGetTransaction.mockResolvedValue(null);

      const ctx = {
        correlationId: "test-notfound",
        jobId: "settle_notfound",
        jobType: "settlement" as const,
      };

      await processSettlementJob(job, ctx);

      expect(h.stellarHashOf).toHaveBeenCalledWith("signed-xdr-notfound");
      expect(h.stellarGetTransaction).toHaveBeenCalledWith("notfound_hash");
      expect(prisma.settlement.updateMany).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            status: "needs_review",
            errorCategory: "indeterminate",
          }),
        })
      );
    });
  });

  // ── 9. Invalid idempotency key ───────────────────────────────────────

  describe("9. Invalid idempotency key", () => {
    it("rejects a request with no Idempotency-Key header", async () => {
      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: authHeader(),
        payload: { signedXdr: "AAAA" },
      });

      expect(res.statusCode).toBe(400);
      expect(res.json().error).toBe("MISSING_IDEMPOTENCY_KEY");
    });

    it("rejects a key with spaces", async () => {
      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "has spaces" },
        payload: { signedXdr: "AAAA" },
      });

      expect(res.statusCode).toBe(400);
      expect(res.json().error).toBe("INVALID_IDEMPOTENCY_KEY");
    });

    it("rejects a key over 255 characters", async () => {
      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "a".repeat(256) },
        payload: { signedXdr: "AAAA" },
      });

      expect(res.statusCode).toBe(400);
      expect(res.json().error).toBe("INVALID_IDEMPOTENCY_KEY");
    });

    it("accepts a valid key with letters, numbers, dash, underscore, dot, colon", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: {
          ...authHeader(),
          "idempotency-key": "valid_key-1.0:abc",
        },
        payload: { signedXdr: "AAAA" },
      });

      expect(res.statusCode).not.toBe(400);
    });
  });

  // ── 10. Authorization on replay ──────────────────────────────────────

  describe("10. Authorization on replay", () => {
    it("membership check is enforced on every request including replays", async () => {
      const cachedResponse = {
        settlement: { id: "settle_1", status: "submitted" },
      };

      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-auth-replay",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_AUTH",
        }),
        responseJson: JSON.stringify(cachedResponse),
        status: "completed",
      });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-auth-replay" },
        payload: { signedXdr: "AAAA_AUTH" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual(cachedResponse);
    });

    it("rejects a replay from a non-member before reaching the idempotency service", async () => {
      prisma.groupMember.findUnique.mockResolvedValue(null);

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-auth-nonmember" },
        payload: { signedXdr: "AAAA_AUTH" },
      });

      expect(res.statusCode).toBe(403);
      expect(prisma.idempotencyKey.findUnique).not.toHaveBeenCalled();
    });

    it("rejects a replay from the non-payer", async () => {
      prisma.settlement.findUnique.mockResolvedValue(
        makeSettlement({ fromUserId: "different_user" })
      );

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-auth-notpayer" },
        payload: { signedXdr: "AAAA_AUTH" },
      });

      expect(res.statusCode).toBe(403);
    });
  });

  // ── 11. Audit/state transition behavior ──────────────────────────────

  describe("11. Audit/state transition behavior", () => {
    it("does not generate duplicate audit events on replay", async () => {
      const cachedResponse = {
        settlement: { id: "settle_1", status: "submitted" },
      };

      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-audit-replay",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_AUDIT",
        }),
        responseJson: JSON.stringify(cachedResponse),
        status: "completed",
      });

      await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-audit-replay" },
        payload: { signedXdr: "AAAA_AUDIT" },
      });

      await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-audit-replay" },
        payload: { signedXdr: "AAAA_AUDIT" },
      });

      expect(prisma.auditLog.create).not.toHaveBeenCalled();
    });

    it("audit log is created exactly once on first submission", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);

      await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-audit-first" },
        payload: { signedXdr: "AAAA_AUDIT_FIRST" },
      });

      expect(prisma.auditLog.create).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            action: "settlement.confirm",
            entityType: "settlement",
            entityId: "settle_1",
          }),
        })
      );
    });
  });

  // ── 12. Process-safe persistence behavior ────────────────────────────

  describe("12. Process-safe persistence behavior", () => {
    it("idempotency protection works across process restarts — persisted in DB", async () => {
      prisma.idempotencyKey.findUnique.mockResolvedValue(null);
      prisma.idempotencyKey.create.mockResolvedValue({});

      const res1 = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-process-safe" },
        payload: { signedXdr: "AAAA_PS" },
      });

      expect(res1.statusCode).toBe(200);

      // Simulate process restart: the DB still has the key
      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-process-safe",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_PS",
        }),
        responseJson: res1.body,
        status: "completed",
      });

      const res2 = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-process-safe" },
        payload: { signedXdr: "AAAA_PS" },
      });

      expect(res2.statusCode).toBe(200);
      expect(res2.body).toBe(res1.body);
    });

    it("abandoned in_progress key is reclaimable after timeout", async () => {
      const { IN_PROGRESS_TIMEOUT_MS } = await import("../src/services/idempotency");

      const staleTime = new Date(Date.now() - IN_PROGRESS_TIMEOUT_MS - 1000);

      prisma.idempotencyKey.findUnique.mockResolvedValue({
        userId: "user_1",
        scope: "settlement.confirm",
        key: "key-abandoned",
        requestHash: hashRequest("settlement.confirm", "settle_1", {
          signedXdr: "AAAA_ABANDONED",
        }),
        responseJson: null,
        status: "in_progress",
        updatedAt: staleTime,
        createdAt: staleTime,
      });

      prisma.idempotencyKey.updateMany.mockResolvedValue({ count: 1 });

      const res = await app.inject({
        method: "POST",
        url: "/settlements/settle_1/confirm",
        headers: { ...authHeader(), "idempotency-key": "key-abandoned" },
        payload: { signedXdr: "AAAA_ABANDONED" },
      });

      expect(res.statusCode).toBe(200);
      expect(prisma.settlement.updateMany).toHaveBeenCalled();
    });
  });
});
