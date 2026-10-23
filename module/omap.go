package module

// omap is a map that iterates in insertion order, as a JavaScript Map
// does: setting a key it holds keeps its place, deleting and setting it
// again puts it last. The journal's state and the index's carriers,
// advertisements, hold outputs and sweeps are kept in one, so that a
// compacted journal lists what it holds in the TypeScript module's order.
type omap[V any] struct {
	m     map[string]oval[V]
	order []okey
	gen   uint64
	dead  int
}

type oval[V any] struct {
	v   V
	gen uint64
}

type okey struct {
	k   string
	gen uint64
}

func newOmap[V any]() *omap[V] { return &omap[V]{m: map[string]oval[V]{}} }

func (o *omap[V]) len() int { return len(o.m) }

func (o *omap[V]) get(k string) (V, bool) {
	x, ok := o.m[k]
	return x.v, ok
}

func (o *omap[V]) has(k string) bool {
	_, ok := o.m[k]
	return ok
}

func (o *omap[V]) set(k string, v V) {
	if x, ok := o.m[k]; ok {
		o.m[k] = oval[V]{v: v, gen: x.gen}
		return
	}
	o.gen++
	o.m[k] = oval[V]{v: v, gen: o.gen}
	o.order = append(o.order, okey{k: k, gen: o.gen})
}

func (o *omap[V]) del(k string) {
	if _, ok := o.m[k]; !ok {
		return
	}
	delete(o.m, k)
	o.dead++
	if o.dead > 64 && o.dead > len(o.order)/2 {
		live := o.order[:0]
		for _, e := range o.order {
			if x, ok := o.m[e.k]; ok && x.gen == e.gen {
				live = append(live, e)
			}
		}
		o.order = live
		o.dead = 0
	}
}

// keys are the keys in insertion order: a copy, safe to change the map
// while walking it.
func (o *omap[V]) keys() []string {
	out := make([]string, 0, len(o.m))
	for _, e := range o.order {
		if x, ok := o.m[e.k]; ok && x.gen == e.gen {
			out = append(out, e.k)
		}
	}
	return out
}

// values are the values in insertion order, a copy.
func (o *omap[V]) values() []V {
	out := make([]V, 0, len(o.m))
	for _, e := range o.order {
		if x, ok := o.m[e.k]; ok && x.gen == e.gen {
			out = append(out, x.v)
		}
	}
	return out
}

func (o *omap[V]) clear() {
	o.m = map[string]oval[V]{}
	o.order = nil
	o.dead = 0
}
