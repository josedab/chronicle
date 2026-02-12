package raft

import "encoding/json"

func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case entry := <-rn.applyCh:
			if entry.Type == RaftLogCommand {
				var cmd RaftCommand
				if err := json.Unmarshal(entry.Data, &cmd); err == nil {
					_ = rn.applyCommand(cmd)
				}
			}
		}
	}
}

func (rn *RaftNode) applyCommitted() {
	rn.stateMu.Lock()
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.stateMu.Unlock()

	for idx := lastApplied + 1; idx <= commitIndex; idx++ {
		entry := rn.log.Get(idx)
		if entry == nil {
			continue
		}

		select {
		case rn.applyCh <- entry:
		case <-rn.ctx.Done():
			return
		}

		rn.stateMu.Lock()
		rn.lastApplied = idx
		rn.stateMu.Unlock()
	}
}

func (rn *RaftNode) applyCommand(cmd RaftCommand) error {
	switch cmd.Op {
	case "write":
		var p Point
		if err := json.Unmarshal(cmd.Value, &p); err != nil {
			return err
		}
		return rn.store.Write(p)
	case "delete":

	}
	return nil
}
