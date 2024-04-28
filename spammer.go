package main

import (
	"sort"
	"strconv"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	if len(cmds) == 0 {
		return
	}
	channels := make([]chan interface{}, len(cmds)+1)
	for i := range channels {
		channels[i] = make(chan interface{})
	}

	var wg sync.WaitGroup

	for i, command := range cmds {
		wg.Add(1)
		go func(i int, in, out chan interface{}, cmd cmd) {
			defer wg.Done()
			cmd(in, out)
			close(out)
		}(i, channels[i], channels[i+1], command)
	}

	close(channels[0])
	//
	//
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	seenUsers := make(map[uint64]struct{})
	for rawEmail := range in {
		email, ok := rawEmail.(string)
		if !ok {continue}

		user := GetUser(email)
		if _, exists := seenUsers[user.ID]; !exists {
			seenUsers[user.ID] = struct{}{}
			out <- user
		}
	}
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup

	batch := make([]User, 0, GetMessagesMaxUsersBatch)

	sendBatch := func(batch []User) {
		defer wg.Done()
		msgIDs, err := GetMessages(batch...)
		if err != nil {
			return
		}
		for _, msgID := range msgIDs {
			out <- msgID
		}
	}

	for user := range in {
		u, ok := user.(User)
		if !ok {
			continue
		}
		batch = append(batch, u)
		if len(batch) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go sendBatch(batch)
			batch = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(batch) > 0 {
		wg.Add(1)
		go sendBatch(batch)
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, HasSpamMaxAsyncRequests)

	processMsg := func(msgID MsgID) {
		defer wg.Done()
		sem <- struct{}{}
		hasSpam, err := HasSpam(msgID)
		<-sem

		if err != nil {
			return
		}
		out <- MsgData{ID: msgID, HasSpam: hasSpam}
	}

	for rawMsgID := range in {
		msgID, ok := rawMsgID.(MsgID)
		if !ok {
			continue
		}

		wg.Add(1)
		go processMsg(msgID)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := []MsgData{}
	for rawMsgData := range in {
		msgData, ok := rawMsgData.(MsgData)
		if !ok {
			continue
		}
		results = append(results, msgData)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam == results[j].HasSpam {
			return results[i].ID < results[j].ID
		}
		return !results[i].HasSpam && results[j].HasSpam
	})

	for _, res := range results {
		output := strconv.FormatBool(res.HasSpam) + " " + strconv.FormatUint(uint64(res.ID), 10)
		out <- output
	}

	close(out)
}
