"""
tna.py — Reference implementation of the Typed Nullity Algebra (S, ⊕, ⊗, /)

Elements of S:
  AbsZero  (0_bm)  — inapplicability / structural absence
  MeasZero (0_m)   — indeterminacy / sub-threshold
  ResToken (1_t)   — resolved comparison
  Real(v)          — standard real number

Author: Stefan Rankovic, 2026
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Union
import math


# ── Element types ──────────────────────────────────────────────────

class AbsZero:
    """Absolute zero: additive identity, multiplicative annihilator."""
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    def __repr__(self): return "0_bm"
    def __eq__(self, other): return isinstance(other, AbsZero)
    def __hash__(self): return hash("AbsZero")

class MeasZero:
    """Measured zero: indeterminacy / sub-threshold."""
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    def __repr__(self): return "0_m"
    def __eq__(self, other): return isinstance(other, MeasZero)
    def __hash__(self): return hash("MeasZero")

class ResToken:
    """Resolution token: additive absorber, multiplicative identity."""
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    def __repr__(self): return "1_t"
    def __eq__(self, other): return isinstance(other, ResToken)
    def __hash__(self): return hash("ResToken")

@dataclass(frozen=True)
class Real:
    """Wrapper for real numbers in S."""
    v: float
    def __repr__(self): return f"Real({self.v})"
    def __eq__(self, other):
        return isinstance(other, Real) and math.isclose(self.v, other.v, abs_tol=1e-12)
    def __hash__(self): return hash(("Real", round(self.v, 12)))


# Singletons
O_BM = AbsZero()
O_M  = MeasZero()
ONE_T = ResToken()

# Type alias
S = Union[AbsZero, MeasZero, ResToken, Real]


# ── Operations ─────────────────────────────────────────────────────

def add(x: S, y: S) -> S:
    """Extended addition (⊕) on S."""
    # A5: Resolution absorption (top absorber)
    if isinstance(x, ResToken) or isinstance(y, ResToken):
        return ONE_T
    # A2: Absolute zero identity
    if isinstance(x, AbsZero):
        return y
    if isinstance(y, AbsZero):
        return x
    # A4: Indeterminacy idempotence
    if isinstance(x, MeasZero) and isinstance(y, MeasZero):
        return O_M
    # A3: Precision dominance
    if isinstance(x, MeasZero) and isinstance(y, Real):
        return y
    if isinstance(x, Real) and isinstance(y, MeasZero):
        return x
    # A1: Real embedding
    if isinstance(x, Real) and isinstance(y, Real):
        return Real(x.v + y.v)
    raise TypeError(f"add: unexpected types {type(x)}, {type(y)}")


def mul(x: S, y: S) -> S:
    """Extended multiplication (⊗) on S."""
    # M2: Absolute annihilation (bottom annihilator)
    if isinstance(x, AbsZero) or isinstance(y, AbsZero):
        return O_BM
    # M4: Real zero collapse: 0_R × 0_m = 0_bm
    if isinstance(x, Real) and x.v == 0 and isinstance(y, MeasZero):
        return O_BM
    if isinstance(y, Real) and y.v == 0 and isinstance(x, MeasZero):
        return O_BM
    # M5: Resolution identity (top = multiplicative identity)
    if isinstance(x, ResToken):
        return y
    if isinstance(y, ResToken):
        return x
    # M3: Indeterminacy propagation
    if isinstance(x, MeasZero) and isinstance(y, MeasZero):
        return O_M
    if isinstance(x, MeasZero) and isinstance(y, Real) and y.v != 0:
        return O_M
    if isinstance(y, MeasZero) and isinstance(x, Real) and x.v != 0:
        return O_M
    # M1: Real embedding
    if isinstance(x, Real) and isinstance(y, Real):
        return Real(x.v * y.v)
    raise TypeError(f"mul: unexpected types {type(x)}, {type(y)}")


def div(x: S, y: S) -> S:
    """Extended division (/) on S — total auxiliary function."""
    # D2: Division by AbsZero
    if isinstance(y, AbsZero):
        return O_BM
    # D5: Division by ResToken
    if isinstance(y, ResToken):
        return x
    # D4: Indeterminate resolution
    if isinstance(x, MeasZero) and isinstance(y, MeasZero):
        return ONE_T
    # D6: Real zero singularity
    if isinstance(x, Real) and x.v == 0 and isinstance(y, Real) and y.v == 0:
        return O_BM
    # D3: Division by MeasZero
    if isinstance(y, MeasZero):
        if isinstance(x, Real) and x.v == 0:
            return O_BM
        if isinstance(x, AbsZero):
            return O_BM
        if isinstance(x, Real) and x.v != 0:
            return O_M
        if isinstance(x, ResToken):
            return O_M
    # D7: Division of MeasZero by reals
    if isinstance(x, MeasZero) and isinstance(y, Real):
        return O_M if y.v != 0 else O_BM
    # D8: Division of ResToken
    if isinstance(x, ResToken) and isinstance(y, Real):
        return ONE_T if y.v != 0 else O_BM
    # D1: Real division
    if isinstance(x, Real) and isinstance(y, Real) and y.v != 0:
        return Real(x.v / y.v)
    # D9: Real / Real(0) where x != 0 — singularity → 0_m
    if isinstance(x, Real) and isinstance(y, Real) and y.v == 0:
        return O_M
    # Division of AbsZero by reals
    if isinstance(x, AbsZero):
        return O_BM
    raise TypeError(f"div: unexpected types {type(x)}, {type(y)}")


# ── Typed Aggregates ───────────────────────────────────────────────

def count_s(M: list[S]) -> int:
    """COUNT_S: count eligible (non-0_bm) entries."""
    return sum(1 for x in M if not isinstance(x, AbsZero))

def sum_s(M: list[S]) -> S:
    """SUM_S: sum of eligible entries under ⊕."""
    eligible = [x for x in M if not isinstance(x, AbsZero)]
    if not eligible:
        return O_BM
    result = eligible[0]
    for x in eligible[1:]:
        result = add(result, x)
    return result

def avg_s(M: list[S]) -> S:
    """AVG_S: typed average with eligibility semantics."""
    k = count_s(M)
    if k == 0:
        return O_BM
    return div(sum_s(M), Real(float(k)))


# ── Tests ──────────────────────────────────────────────────────────

def test_monoid_identity():
    """Verify additive identity (0_bm) and multiplicative identity (1_t)."""
    elements = [O_BM, O_M, ONE_T, Real(0), Real(5), Real(-3.7)]
    for x in elements:
        assert add(x, O_BM) == x, f"add identity failed for {x}"
        assert add(O_BM, x) == x, f"add identity (left) failed for {x}"
        assert mul(x, ONE_T) == x, f"mul identity failed for {x}"
        assert mul(ONE_T, x) == x, f"mul identity (left) failed for {x}"
    print("  [PASS] Monoid identities")

def test_commutativity():
    """Verify commutativity of ⊕ and ⊗."""
    elements = [O_BM, O_M, ONE_T, Real(0), Real(3), Real(-2)]
    for x in elements:
        for y in elements:
            assert add(x, y) == add(y, x), f"add not commutative: {x}, {y}"
            assert mul(x, y) == mul(y, x), f"mul not commutative: {x}, {y}"
    print("  [PASS] Commutativity")

def test_associativity():
    """Verify associativity of ⊕ and ⊗."""
    elements = [O_BM, O_M, ONE_T, Real(0), Real(2), Real(-1)]
    for x in elements:
        for y in elements:
            for z in elements:
                lhs_a = add(add(x, y), z)
                rhs_a = add(x, add(y, z))
                assert lhs_a == rhs_a, f"add assoc failed: ({x}⊕{y})⊕{z} = {lhs_a} ≠ {rhs_a} = {x}⊕({y}⊕{z})"
                lhs_m = mul(mul(x, y), z)
                rhs_m = mul(x, mul(y, z))
                assert lhs_m == rhs_m, f"mul assoc failed: ({x}⊗{y})⊗{z} = {lhs_m} ≠ {rhs_m} = {x}⊗({y}⊗{z})"
    print("  [PASS] Associativity")

def test_absorption():
    """Verify absorption properties: 1_t absorbs under ⊕, 0_bm annihilates under ⊗."""
    elements = [O_BM, O_M, ONE_T, Real(0), Real(7), Real(-4)]
    for x in elements:
        assert add(x, ONE_T) == ONE_T, f"1_t not additive absorber for {x}"
        assert mul(x, O_BM) == O_BM, f"0_bm not multiplicative annihilator for {x}"
    print("  [PASS] Absorption properties")

def test_distinguishability():
    """Verify 1_t is distinct from all reals under ⊕."""
    assert add(ONE_T, ONE_T) == ONE_T, "1_t ⊕ 1_t should be 1_t"
    assert add(Real(1), Real(1)) == Real(2), "1 + 1 should be 2"
    assert ONE_T != Real(1), "1_t should not equal Real(1)"
    assert ONE_T != Real(2), "1_t should not equal Real(2)"
    print("  [PASS] Distinguishability of 1_t")

def test_coherence():
    """Verify D4 coherence: (0_m / 0_m) ⊗ 0_m = 0_m."""
    result = mul(div(O_M, O_M), O_M)
    assert result == O_M, f"Coherence failed: got {result}"
    print("  [PASS] Division coherence (T6)")

def test_distributivity_holds():
    """Verify distributivity on cancellation-free triples."""
    # x=5, y=3, z=2 (all reals, no cancellation)
    x, y, z = Real(5), Real(3), Real(2)
    assert mul(x, add(y, z)) == add(mul(x, y), mul(x, z))
    # x=0_bm (annihilator)
    assert mul(O_BM, add(Real(3), O_M)) == add(mul(O_BM, Real(3)), mul(O_BM, O_M))
    # x=1_t (identity)
    assert mul(ONE_T, add(Real(3), O_M)) == add(mul(ONE_T, Real(3)), mul(ONE_T, O_M))
    print("  [PASS] Distributivity (cancellation-free)")

def test_distributivity_fails():
    """Verify the two known failure classes."""
    # CF1: x=0_m, y=3, z=-3 (cancellation)
    x, y, z = O_M, Real(3), Real(-3)
    lhs = mul(x, add(y, z))   # 0_m ⊗ 0_R = 0_bm
    rhs = add(mul(x, y), mul(x, z))  # 0_m ⊕ 0_m = 0_m
    assert lhs != rhs, "CF1 failure should occur"
    assert lhs == O_BM and rhs == O_M

    # CF2: x=5, y=3, z=1_t (absorption-magnitude conflict)
    x, y, z = Real(5), Real(3), ONE_T
    lhs = mul(x, add(y, z))   # 5 ⊗ 1_t = 5
    rhs = add(mul(x, y), mul(x, z))  # 15 ⊕ 5 = 20
    assert lhs != rhs, "CF2 failure should occur"
    print("  [PASS] Distributivity failures (CF1, CF2)")

def test_monotonicity():
    """Verify monotonicity of ⊕ and ⊗ on typed chain 0_bm ⊑ 0_m ⊑ 1_t."""
    chain = [O_BM, O_M, ONE_T]
    test_elements = [O_BM, O_M, ONE_T, Real(0), Real(5), Real(-2)]
    for s in test_elements:
        add_results = [add(s, t) for t in chain]
        mul_results = [mul(s, t) for t in chain]
        # Check non-decreasing under ⊑ (level function)
        def level(x):
            if isinstance(x, AbsZero): return 0
            if isinstance(x, MeasZero): return 1
            if isinstance(x, Real): return 2
            if isinstance(x, ResToken): return 3
        for results, op_name in [(add_results, "⊕"), (mul_results, "⊗")]:
            levels = [level(r) for r in results]
            for i in range(len(levels) - 1):
                assert levels[i] <= levels[i+1], \
                    f"Monotonicity failed: {s} {op_name} {chain[i]}={results[i]} (L{levels[i]}) > " \
                    f"{s} {op_name} {chain[i+1]}={results[i+1]} (L{levels[i+1]})"
    print("  [PASS] Monotonicity")

def test_aggregates():
    """Test typed aggregation operators."""
    # Example 7.1: Commission averaging
    M = [Real(500), O_M, O_BM]
    assert count_s(M) == 2
    assert sum_s(M) == Real(500)
    assert avg_s(M) == Real(250)

    # Example 7.2: Inventory
    M2 = [Real(50), Real(30), O_M, O_BM, O_M]
    assert count_s(M2) == 4
    assert sum_s(M2) == Real(80)
    assert avg_s(M2) == Real(20)

    # Example 7.3: All inapplicable
    M3 = [O_BM, O_BM, O_BM]
    assert count_s(M3) == 0
    assert sum_s(M3) == O_BM
    assert avg_s(M3) == O_BM

    print("  [PASS] Typed aggregates")


if __name__ == "__main__":
    print("Typed Nullity Algebra — Reference Implementation Tests")
    print("=" * 55)
    test_monoid_identity()
    test_commutativity()
    test_associativity()
    test_absorption()
    test_distinguishability()
    test_coherence()
    test_distributivity_holds()
    test_distributivity_fails()
    test_monotonicity()
    test_aggregates()
    print("=" * 55)
    print("All tests passed.")
