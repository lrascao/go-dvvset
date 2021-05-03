package dvvset

type Clock struct {
	entries []clock_entry
	values  Values
}

type Vector struct {
	id      ServerId
	counter Counter
}

type clock_entry struct {
	id      ServerId
	counter Counter
	values  Values
}

type ServerId string
type Value interface{}
type Counter = int
type Values []Value
