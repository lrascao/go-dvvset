package dvvset

import (
	"testing"

	"github.com/tj/assert"
)

func TestJoin(t *testing.T) {
	a := New("v1")
	assert.Equal(t, []Vector(nil), a.Join())

	a.Update("a")
	b := NewWithContext(a.Join(), "v2")
	b.Update1(a, "b")

	assert.Equal(t, []Vector{Vector{id: "a", counter: 1}}, a.Join())
	assert.Equal(t, []Vector{Vector{id: "a", counter: 1}, Vector{id: "b", counter: 1}}, b.Join())
	assert.Equal(t, Values{"v2"}, b.Values())
}

func TestServerIdSorting(t *testing.T) {
	assert.True(t, "n1" < "n2")
	assert.True(t, "n2" > "n1")
}

func TestEvent(t *testing.T) {
	// switch {
	// case head.id == id:
	assert.Equal(t,
		[]clock_entry{
			clock_entry{id: "n1", counter: 2, values: Values{"v2", "v1"}},
			clock_entry{id: "n2", counter: 1, values: Values{"v3"}}},
		event([]clock_entry{
			clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			clock_entry{id: "n2", counter: 1, values: Values{"v3"}}}, "n1", "v2"))

	// case head.id > id:
	// event([{I1, _, _} | _]=C, I, V) when I1 > I -> [{I, 1, [V]} | C];
	assert.Equal(t,
		[]clock_entry{
			clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			clock_entry{id: "n2", counter: 3, values: Values{"v2"}},
			clock_entry{id: "n3", counter: 4, values: Values{"v3"}},
		},
		event([]clock_entry{
			clock_entry{id: "n2", counter: 3, values: Values{"v2"}},
			clock_entry{id: "n3", counter: 4, values: Values{"v3"}},
		}, "n1", "v1"))

	// default:
	// event([H | T], I, V) -> [H | event(T, I, V)].
	assert.Equal(t,
		[]clock_entry{
			clock_entry{id: "n2", counter: 3, values: Values{"v2"}},
			clock_entry{id: "n3", counter: 4, values: Values{"v3"}},
			clock_entry{id: "n5", counter: 1, values: Values{"v5"}},
		},
		event([]clock_entry{
			clock_entry{id: "n2", counter: 3, values: Values{"v2"}},
			clock_entry{id: "n3", counter: 4, values: Values{"v3"}},
		}, "n5", "v5"))
}

func TestGreater(t *testing.T) {

	for _, ti := range []struct {
		title  string
		c1     []clock_entry
		c2     []clock_entry
		strict bool
		want   bool
	}{
		{
			title:  "Always get strict on empty args",
			c1:     []clock_entry{},
			c2:     []clock_entry{},
			strict: false,
			want:   false,
		},
		{
			title:  "Always get strict on empty args",
			c1:     []clock_entry{},
			c2:     []clock_entry{},
			strict: true,
			want:   true,
		},

		{
			title: "Always true on empty second arg",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			c2:     []clock_entry{},
			strict: false,
			want:   true,
		},
		{
			title: "Always true on empty second arg",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			c2:     []clock_entry{},
			strict: true,
			want:   true,
		},

		{
			title: "Always false on empty first arg",
			c1:    []clock_entry{},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			strict: false,
			want:   false,
		},
		{
			title: "Always false on empty first arg",
			c1:    []clock_entry{},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			strict: false,
			want:   false,
		},

		{
			title: "Exact same vectors",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			strict: false,
			want:   false,
		},
		{
			title: "Exact same vectors",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			strict: true,
			want:   true,
		},

		{
			title: "Same server id, first vector is greater than the second",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n2", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			strict: false,
			want:   true,
		},

		{
			title: "Same server id, second vector is greater than the first",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
				clock_entry{id: "n2", counter: 2, values: Values{"v1"}},
			},
			strict: true,
			want:   false,
		},

		{
			title: "First vector is greater than the second, lower server id",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n2", counter: 1, values: Values{"v1"}},
			},
			strict: true,
			want:   false,
		},
		{
			title: "First vector is greater than the second, lower server id",
			c1: []clock_entry{
				clock_entry{id: "n1", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n2", counter: 1, values: Values{"v1"}},
			},
			strict: false,
			want:   false,
		},

		{
			title: "First vector is greater than the second, higher server id",
			c1: []clock_entry{
				clock_entry{id: "n2", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			strict: true,
			want:   false,
		},
		{
			title: "First vector is greater than the second, higher server id",
			c1: []clock_entry{
				clock_entry{id: "n2", counter: 2, values: Values{"v1"}},
			},
			c2: []clock_entry{
				clock_entry{id: "n1", counter: 1, values: Values{"v1"}},
			},
			strict: false,
			want:   false,
		},
	} {
		t.Run(ti.title, func(t *testing.T) {
			assert.Equal(t,
				ti.want,
				greater(ti.c1, ti.c2, ti.strict))
		})
	}
}

func TestLess(t *testing.T) {
	a := New("v1")
	a.Update("a")

	b := NewWithContext(a.Join(), "v2")
	b.Update("a")

	b2 := NewWithContext(a.Join(), "v2")
	b2.Update("b")

	b3 := NewWithContext(a.Join(), "v2")
	b3.Update("z")

	c := NewWithContext(b.Join(), "v3")
	c.Update1(a, "c")

	d := NewWithContext(c.Join(), "v4")
	d.Update1(b2, "d")

	assert.True(t, a.Less(b))
	assert.True(t, a.Less(c))
	assert.True(t, b.Less(c))
	assert.True(t, b.Less(d))
	assert.True(t, b2.Less(d))
	assert.True(t, a.Less(d))

	assert.False(t, b2.Less(c))
	assert.False(t, b.Less(b2))
	assert.False(t, b2.Less(b))
	assert.False(t, a.Less(a))
	assert.False(t, c.Less(c))
	assert.False(t, d.Less(b2))
	assert.False(t, b3.Less(d))
}

func TestUpdate(t *testing.T) {
	a0 := New("v1")
	a0.Update("a")

	a1 := NewWithContext(a0.Join(), "v2")
	a1.Update1(a0, "a")

	a2 := NewWithContext(a1.Join(), "v3")
	a2.Update1(a1, "b")

	a3 := NewWithContext(a0.Join(), "v4")
	a3.Update1(a1, "b")

	a4 := NewWithContext(a0.Join(), "v5")
	a4.Update1(a1, "a")

	assert.Equal(t, Values{"v5", "v2"}, a4.Values())
	assert.Equal(t,
		Clock{entries: []clock_entry{clock_entry{id: "a", counter: 1, values: Values{"v1"}}},
			values: Values{}},
		a0)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{"v2"}},
			},
			values: Values{}},
		a1)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{}},
				clock_entry{id: "b", counter: 1, values: Values{"v3"}},
			},
			values: Values{}},
		a2)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{"v2"}},
				clock_entry{id: "b", counter: 1, values: Values{"v4"}},
			},
			values: Values{}},
		a3)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 3, values: Values{"v5", "v2"}},
			},
			values: Values{}},
		a4)
}

func TestSyncUpdate(t *testing.T) {
	a0 := New("v1")
	a0.Update("a")

	a1 := New("v2")
	a1.Update1(a0, "a")

	a2 := NewWithContext(a0.Join(), "v3")
	a2.Update1(a1, "a")

	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 1, values: Values{"v1"}},
			},
			values: Values{}},
		a0)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{"v2", "v1"}},
			},
			values: Values{}},
		a1)
	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 3, values: Values{"v3", "v2"}},
			},
			values: Values{}},
		a2)
}

func TestSync(t *testing.T) {
	x := Clock{
		entries: []clock_entry{
			clock_entry{id: "x", counter: 1, values: Values{}},
		},
		values: Values{},
	}

	a := New("v1")
	a.Update("a")

	y := New("v2")
	y.Update("b")

	a1 := NewWithContext(a.Join(), "v2")
	a1.Update("a")

	a3 := NewWithContext(a1.Join(), "v3")
	a3.Update("b")

	a4 := NewWithContext(a1.Join(), "v3")
	a4.Update("c")

	w := Clock{
		entries: []clock_entry{
			clock_entry{id: "a", counter: 1, values: Values{}},
		},
		values: Values{},
	}

	z := Clock{
		entries: []clock_entry{
			clock_entry{id: "a", counter: 2, values: Values{"v2", "v1"}},
		},
		values: Values{},
	}

	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{"v2"}},
			},
			values: Values{},
		},
		sync2(w, z))
	assert.Equal(t,
		Sync([]Clock{w, z}),
		Sync([]Clock{z, w}))
	assert.Equal(t,
		Sync([]Clock{a, a1}),
		Sync([]Clock{a1, a}))

	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 2, values: Values{}},
				clock_entry{id: "b", counter: 1, values: Values{"v3"}},
				clock_entry{id: "c", counter: 1, values: Values{"v3"}},
			},
			values: Values{},
		},
		Sync([]Clock{a4, a3}))
	assert.Equal(t,
		Sync([]Clock{a4, a3}),
		Sync([]Clock{a3, a4}))

	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 1, values: Values{"v1"}},
				clock_entry{id: "x", counter: 1, values: Values{}},
			},
			values: Values{},
		},
		Sync([]Clock{x, a}))
	assert.Equal(t,
		Sync([]Clock{x, a}),
		Sync([]Clock{a, x}))
	assert.Equal(t,
		Sync([]Clock{a, x}),
		Sync([]Clock{x, a}))

	assert.Equal(t,
		Clock{
			entries: []clock_entry{
				clock_entry{id: "a", counter: 1, values: Values{"v1"}},
				clock_entry{id: "b", counter: 1, values: Values{"v2"}},
			},
			values: Values{},
		},
		Sync([]Clock{a, y}))
	assert.Equal(t,
		Sync([]Clock{y, a}),
		Sync([]Clock{a, y}))
	assert.Equal(t,
		Sync([]Clock{a, y}),
		Sync([]Clock{y, a}))

}

func TestScenario1(t *testing.T) {
	// C1 client writes v1 to node n1, then reads
	// write v1
	a0 := New("v1")
	a0.Update("n1")
	// read, result of the read is a context
	c1Context0 := a0.Join()
	assert.Equal(t, Values{"v1"}, a0.Values())

	// C2 writes v2 on node n1 without having any causal information
	a1 := New("v2")
	a1.Update1(a0, "n1")

	// C1 writes v3 to n1 without having read first,
	// it's still using the previous context it got on the first read
	a2 := NewWithContext(c1Context0, "v3")
	a2.Update1(a1, "n1")
	// C1 reads, gets a new context
	c1Context1 := a2.Join()
	assert.Equal(t, Values{"v3", "v2"}, a2.Values())

	// C2 writes v2 on node n1 without having any causal information
	a3 := New("v4")
	a3.Update1(a2, "n1")

	// C1 writes v5 on node v1
	a4 := NewWithContext(c1Context1, "v5")
	a4.Update1(a3, "n1")

	// C1 reads
	assert.Equal(t, Values{"v5", "v4"}, a4.Values())
}

func TestScenario2(t *testing.T) {
	// % C1 client writes v1, then reads
	// DVV0 = dvvset:update(dvvset:new(v1), n1),
	// % read, result of the read is a {context, value} tuple
	// {C1Context0, C1Values0} = {dvvset:join(DVV0), dvvset:values(DVV0)},
	// ?assertEqual(lists:sort(C1Values0), [v1]),

	// % C2 client writes v2, then reads
	// DVV1 = dvvset:update(dvvset:new(v2), DVV0, n1),
	// % read, result of the read is a {context, value} tuple
	// {C2Context0, C2Values0} = {dvvset:join(DVV1), dvvset:values(DVV1)},
	// ?assertEqual(lists:sort(C2Values0), [v1, v2]),

	// % C1 client writes v3, then reads
	// DVV2 = dvvset:update(dvvset:new(C1Context0, v3), DVV1, n1),
	// {C1Context1, C1Values1} = {dvvset:join(DVV2), dvvset:values(DVV2)},
	// ?assertEqual(lists:sort(C1Values1), [v2, v3]),

	// % C2 client writes v4, then reads
	// DVV3 = dvvset:update(dvvset:new(C2Context0, v4), DVV2, n1),
	// % read, result of the read is a {context, value} tuple
	// {_C2Context1, C2Values1} = {dvvset:join(DVV3), dvvset:values(DVV3)},
	// ?assertEqual(lists:sort(C2Values1), [v3, v4]),

	// % C1 client writes v5, then reads
	// DVV4 = dvvset:update(dvvset:new(C1Context1, v5), DVV3, n1),
	// {_C1Context2, C1Values2} = {dvvset:join(DVV4), dvvset:values(DVV4)},
	// ?assertEqual(lists:sort(C1Values2), [v4, v5]),

	// % finally
	// % C2 client reads, then writes v6
	// {C2Context2, C2Values2} = {dvvset:join(DVV4), dvvset:values(DVV4)},
	// ?assertEqual(lists:sort(C2Values2), [v4, v5]),
	// DVV5 = dvvset:update(dvvset:new(C2Context2, v6), DVV4, n1),

	// % because it had all causal information upon the write,
	// % v6 will be the final value
	// ?assertEqual(lists:sort(dvvset:values(DVV5)), [v6]),
}
