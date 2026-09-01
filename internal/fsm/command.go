package fsm

import "encoding/json"

// Op identifies the kind of mutation a Command represents.
type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DELETE"
)

// Command is the payload encoded into every Raft log entry.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode serializes the command for use as a raft.Log.Data payload.
func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes a raft.Log.Data payload back into a Command.
func DecodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, err
	}
	return c, nil
}
