package raft


const (
	AppendEntries = "appendEntries"
	RequestVote = "requestVote"
	StateChange = "stateChange"
	ElectionTimeout = "electionTimeout"
)

type Event struct {
	eventType string
	req interface{}
	res interface{}
}

func (e *Event) Type() string {
	return e.eventType
}

func (e *Event) Request() interface{} {
	return e.req
}

func (e *Event) Response() interface{} {
	return nil
}

func NewEvent(eventType string, req interface {}, resp interface{}) Event {
	return Event{eventType: eventType, req: req, res: resp}
}
