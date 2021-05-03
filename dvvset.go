package dvvset

// New creates a new DVV set
func New(v Value) Clock {
	return Clock{
		entries: nil,
		values:  []Value{v},
	}
}

// NewWithContext constructs a new clock set with the causal history
// of the given version vector / vector clock,
// and receives one value that goes to the anonymous list.
// The version vector SHOULD BE the output of join/1.
func NewWithContext(vv []Vector, v Value) Clock {
	var ce []clock_entry
	// TODO: sort for defense against non-order preserving serialization
	for _, v := range vv {
		ce = append(ce, clock_entry{id: v.id, counter: v.counter, values: Values{}})
	}
	return Clock{
		entries: ce,
		values:  []Value{v},
	}
}

// Values returns all the values used in this clock set,
// including the anonymous values.
func (c Clock) Values() Values {
	var anon_values Values
	for _, entry := range c.entries {
		anon_values = append(anon_values, entry.values...)
	}
	return append(c.values, anon_values...)
}

// Join returns a version vector that represents the causal history.
func (c Clock) Join() []Vector {
	var vv []Vector
	for _, e := range c.entries {
		vv = append(vv, Vector{id: e.id, counter: e.counter})
	}
	return vv
}

// Update advances the causal history with the given id.
// The new value is the *anonymous dot* of the clock.
// The client clock SHOULD BE a direct result of new/2.
func (c *Clock) Update(id ServerId) {
	c.entries = event(c.entries, id, c.values[0])
	c.values = Values{}
}

// Update1 advances the causal history of the
// first clock with the given id, while synchronizing
// with the second clock, thus the new clock is
// causally newer than both clocks in the argument.
// The new value is the *anonymous dot* of the clock.
// The first clock SHOULD BE a direct result of new/2,
// which is intended to be the client clock with
// the new value in the *anonymous dot* while
// the second clock is from the local server.
func (c *Clock) Update1(cr Clock, id ServerId) {

	// Sync both clocks without the new value
	c1 := sync2(Clock{entries: c.entries, values: Values{}}, cr)
	// We create a new event on the synced causal history,
	// with id and the new value.
	// The anonymous values that were synced still remain.
	c.entries = event(c1.entries, id, c.values[0])
	c.values = c1.values
}

// %% @doc Synchronizes a list of clocks using sync/2.
// %% It discards (causally) outdated values,
// %% while merging all causal histories.
func Sync(cs []Clock) Clock {
	return sync(cs, Clock{entries: []clock_entry{}, values: Values{}})
}

// %% @doc Returns True if the first clock (c1) is causally older than
// %% the second clock (c2), thus values on the first clock are outdated.
// %% Returns False otherwise.
func (c1 Clock) Less(c2 Clock) bool {
	return greater(c2.entries, c1.entries, false)
}

// Private functions
func event(ces []clock_entry, id ServerId, v Value) []clock_entry {
	// event([], I, V) -> [{I, 1, [V]}];
	if len(ces) == 0 {
		return []clock_entry{clock_entry{id: id,
			counter: 1,
			values:  Values{v}}}
	}

	// pop first element
	head, rest := ces[0], ces[1:]

	switch {
	// event([{I, N, L} | T], I, V) -> [{I, N+1, [V | L]} | T];
	case head.id == id:
		head1 := clock_entry{
			id:      id,
			counter: head.counter + 1,
			values:  append(Values{v}, head.values...)}
		return append([]clock_entry{head1}, rest...)
	// event([{I1, _, _} | _]=C, I, V) when I1 > I -> [{I, 1, [V]} | C];
	case head.id > id:
		return append([]clock_entry{clock_entry{id: id, counter: 1, values: Values{v}}}, ces...)
	// event([H | T], I, V) -> [H | event(T, I, V)].
	default:
		return append([]clock_entry{head}, event(rest, id, v)...)
	}
}

func greater(c1, c2 []clock_entry, strict bool) bool {
	lc1 := len(c1)
	lc2 := len(c2)
	// greater([], [], Strict) -> Strict;
	if lc1 == 0 && lc2 == 0 {
		return strict
	}
	// greater([_|_], [], _) -> true;
	if lc1 != 0 && lc2 == 0 {
		return true
	}
	// greater([], [_|_], _) -> false;
	if lc1 == 0 && lc2 != 0 {
		return false
	}

	// pop head from both lists
	head1, rest1 := c1[0], c1[1:]
	head2, rest2 := c2[0], c2[1:]

	switch {
	case head1.id == head2.id:
		switch {
		case head1.counter == head2.counter:
			return greater(rest1, rest2, strict)
		case head1.counter > head2.counter:
			return greater(rest1, rest2, true)
		default: // head1.counter < head2.counter
			return false
		}
	case head1.id < head2.id:
		return greater(rest1, c2, true)
	default: // head1.id >= head2.id
		return false
	}
}

func sync(cs []Clock, acc Clock) Clock {
	if len(cs) == 0 {
		return acc
	}

	head, rest := cs[0], cs[1:]

	return sync(rest, sync2(head, acc))
}

func unique(v []Value) []Value {
	keys := make(map[Value]bool, len(v))
	list := Values{}
	for _, entry := range v {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func sync2(c1 Clock, c2 Clock) Clock {
	v := Values{}
	if c1.Less(c2) {
		v = append(v, c2.values...)
	} else {
		if c2.Less(c1) {
			v = append(v, c1.values...)
		} else {
			v = c1.values
			v = unique(append(v, c2.values...))
		}
	}

	return Clock{entries: sync_entries(c1.entries, c2.entries),
		values: v}
}

func sync_entries(ce1 []clock_entry, ce2 []clock_entry) []clock_entry {
	lce1 := len(ce1)
	lce2 := len(ce2)

	if lce1 == 0 {
		return ce2
	}
	if lce2 == 0 {
		return ce1
	}

	// pop head from both lists
	head1, rest1 := ce1[0], ce1[1:]
	head2, rest2 := ce2[0], ce2[1:]

	switch {
	case head1.id < head2.id:
		return append([]clock_entry{head1}, sync_entries(rest1, ce2)...)
	case head1.id > head2.id:
		return append([]clock_entry{head2}, sync_entries(rest2, ce1)...)
	default:
		id, counter, values := merge(head1.id, head1.counter, head1.values, head2.counter, head2.values)
		return append([]clock_entry{clock_entry{id: id, counter: counter, values: values}}, sync_entries(rest1, rest2)...)
	}
}

func merge(id ServerId, c1 Counter, vs1 Values, c2 Counter, vs2 Values) (ServerId, Counter, Values) {
	lvs1 := len(vs1)
	lvs2 := len(vs2)

	if c1 >= c2 {
		if c1-lvs1 >= c2-lvs2 {
			return id, c1, vs1
		}
		return id, c1, vs1[0:(c1 - c2 + lvs2)]
	} else {
		if c2-lvs2 >= c1-lvs1 {
			return id, c2, vs2
		}
		return id, c2, vs2[0:(c2 - c1 + lvs1)]
	}

}
