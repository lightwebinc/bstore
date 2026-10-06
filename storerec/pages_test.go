package storerec

import (
	"sort"
	"strings"
	"testing"
)

// The page checks of spec section 11.3 against an oracle: a line-for-line
// port of the brute force the contract's review proved them with
// (pages.py, its fixed variant): random maps of up to six keys over a
// two-letter alphabet, every prefix, cursor, start, page and role
// combination, with a page maximum of 2. Here the enumeration is exhaustive
// over every set of one to six keys of length one to three, not sampled.
// The production check must agree with the oracle's check on every page,
// accept no page the oracle calls unsound, and accept every honest page.

const oracleMax = 2

func oracleKeys() []string {
	var out []string
	var rec func(p string)
	rec = func(p string) {
		if len(p) > 0 {
			out = append(out, p)
		}
		if len(p) == 3 {
			return
		}
		for _, c := range "ab" {
			rec(p + string(c))
		}
	}
	rec("")
	sort.Strings(out)
	return out
}

func oIn(k, p, a string) bool    { return strings.HasPrefix(k, p) && k > a }
func oBelow(k, p, a string) bool { return k <= a || k < p }
func oAbove(k, p, a string) bool { return k > a && !strings.HasPrefix(k, p) && k > p }

// oracleCheck is pages.py's check(..., fixed=True); -1 is None.
func oracleCheck(K []string, p, a string, ans []int, r0, r1, r2 int) bool {
	n := len(K)
	s := n
	switch {
	case len(ans) > 0:
		s = ans[0]
	case r2 >= 0:
		s = r2
	case r1 >= 0:
		s = r1
	}
	for _, x := range ans {
		if !oIn(K[x], p, a) {
			return false
		}
	}
	for i, x := range ans {
		if x != s+i {
			return false
		}
	}
	if (r0 >= 0) != (s > 0) {
		return false
	}
	if r0 >= 0 {
		if r0 != s-1 {
			return false
		}
		k := K[r0]
		if oIn(k, p, a) || !oBelow(k, p, a) {
			return false
		}
	}
	nxt := s
	if len(ans) > 0 {
		nxt = ans[len(ans)-1] + 1
	}
	if r2 >= 0 {
		if r2 != nxt || !oIn(K[r2], p, a) || r1 >= 0 {
			return false
		}
	}
	full := len(ans) == oracleMax
	if !full && r2 < 0 {
		if (r1 >= 0) != (nxt < n) {
			return false
		}
		if r1 >= 0 && (r1 != nxt || !oAbove(K[r1], p, a)) {
			return false
		}
	}
	return true
}

// oracleSound is pages.py's sound.
func oracleSound(K []string, p, a string, ans []int, r2 int) bool {
	var R []int
	for i, k := range K {
		if oIn(k, p, a) {
			R = append(R, i)
		}
	}
	full := len(ans) == oracleMax
	if len(R) == 0 {
		return len(ans) == 0 && r2 < 0
	}
	if len(ans) > 0 {
		if ans[0] != R[0] {
			return false
		}
	} else if r2 >= 0 {
		return r2 == R[0]
	} else {
		return false
	}
	if r2 < 0 && !full {
		if len(ans) != len(R) {
			return false
		}
		for i := range ans {
			if ans[i] != R[i] {
				return false
			}
		}
	}
	return true
}

func pk(K []string, i int) *PageKey {
	if i < 0 {
		return nil
	}
	return &PageKey{Index: uint64(i), Key: K[i]}
}

func TestPageChecksAgainstOracle(t *testing.T) {
	if testing.Short() {
		t.Skip("exhaustive")
	}
	U := oracleKeys()
	prefixes := append([]string{""}, U[:6]...)
	afters := append([]string{""}, U[:8]...)
	tested, unsound, disagree, honestFail := 0, 0, 0, 0
	var subsets func(start int, cur []string)
	subsets = func(start int, cur []string) {
		if len(cur) > 0 {
			K := cur
			n := len(K)
			for _, p := range prefixes {
				for _, a := range afters {
					for st := 0; st <= n; st++ {
						for m := 0; m <= oracleMax && st+m <= n; m++ {
							ans := make([]int, m)
							answered := make([]PageKey, m)
							for i := range ans {
								ans[i] = st + i
								answered[i] = PageKey{Index: uint64(st + i), Key: K[st+i]}
							}
							nx := st + m
							r0s := []int{-1}
							if st > 0 {
								r0s = append(r0s, st-1)
							}
							rn := []int{-1}
							if nx < n {
								rn = append(rn, nx)
							}
							for _, r0 := range r0s {
								for _, r2 := range rn {
									for _, r1 := range rn {
										tested++
										want := oracleCheck(K, p, a, ans, r0, r1, r2)
										got := CheckPageOrder(uint64(n), p, a, oracleMax, answered, pk(K, r0), pk(K, r1), pk(K, r2)) == nil
										if got != want {
											disagree++
										}
										if got && !oracleSound(K, p, a, ans, r2) {
											unsound++
										}
									}
								}
							}
						}
					}
					// The honest page passes.
					m := Map{}
					for _, k := range K {
						m[k] = [32]byte{1}
					}
					entries, leaves := m.ProvePage(p, a, oracleMax, func([32]byte) bool { return true })
					var r0, r1, r2 *PageKey
					for _, l := range leaves {
						switch l.Role {
						case RoleBefore:
							r0 = pageKey(&l)
						case RoleAfter:
							r1 = pageKey(&l)
						case RoleUnheld:
							r2 = pageKey(&l)
						}
					}
					answered := make([]PageKey, len(entries))
					for i, e := range entries {
						answered[i] = PageKey{Index: e.Ctx.Index, Key: e.Key}
					}
					if CheckPageOrder(uint64(n), p, a, oracleMax, answered, r0, r1, r2) != nil {
						honestFail++
					}
				}
			}
		}
		if len(cur) == 6 {
			return
		}
		for i := start; i < len(U); i++ {
			subsets(i+1, append(append([]string(nil), cur...), U[i]))
		}
	}
	subsets(0, nil)
	t.Logf("pages tested %d, disagreements %d, unsound accepted %d, honest refused %d", tested, disagree, unsound, honestFail)
	if disagree != 0 || unsound != 0 || honestFail != 0 {
		t.Fatal("page checks differ from the oracle")
	}
}
