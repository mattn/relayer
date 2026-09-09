package relayer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func TestCloseCancelsPendingHistory(t *testing.T) {
	testPendingHistoryCancellation(t, "CLOSE")
}

func TestLateHistoryCannotRestoreClosedOrReplacedListener(t *testing.T) {
	for _, operation := range []string{"close", "replace", "disconnect"} {
		t.Run(operation, func(t *testing.T) {
			srv := newListenerTestServer()
			ws := &WebSocket{}
			old := srv.beginRequest(context.Background(), ws, "sub")
			var replacement *subscriptionRequest
			switch operation {
			case "close":
				srv.removeListenerId(ws, "sub")
			case "replace":
				replacement = srv.beginRequest(context.Background(), ws, "sub")
				srv.registerRequest(ws, "sub", replacement, nostr.Filters{{Kinds: []int{2}}})
			case "disconnect":
				srv.removeListener(ws)
			}
			// Even a backend that ignores cancellation cannot commit old filters.
			srv.registerRequest(ws, "sub", old, nostr.Filters{{Kinds: []int{1}}})
			srv.finishRequest(ws, "sub", old)
			if operation == "replace" {
				if ws.requests["sub"] != replacement {
					t.Fatal("old worker removed the replacement's request state")
				}
				filters := srv.GetListeningFilters()
				if len(filters) != 1 || len(filters[0].Kinds) != 1 || filters[0].Kinds[0] != 2 {
					t.Fatalf("old request overwrote replacement: %v", filters)
				}
				srv.finishRequest(ws, "sub", replacement)
			} else if totalConnections(srv) != 0 {
				t.Fatal("late history restored a removed listener")
			}
			if len(ws.requests) != 0 {
				t.Fatal("finished requests retained bookkeeping")
			}
		})
	}
}

func TestRequestCancellationStopsUnfinishedStream(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			// The real connection exercises historical EVENT/EOSE ordering and
			// live delivery after closing and reusing the subscription ID.
			started := make(chan context.Context, 1)
			stream := make(chan *nostr.Event, 1)
			stream <- &nostr.Event{ID: "history", Kind: 1}
			storage := &testStorage{queryEvents: func(ctx context.Context, _ nostr.Filter) (chan *nostr.Event, error) {
				started <- ctx
				return stream, nil
			}}
			srv := startTestRelay(t, &testRelay{storage: storage})
			defer srv.Shutdown(context.Background())
			defer close(stream)
			conn := dialWS(t, srv.Addr)
			sendJSON(t, conn, []any{"REQ", "sub", nostr.Filter{Kinds: []int{1}, Limit: limit}})
			if typ, _ := recvMessage(t, conn); typ != "EVENT" {
				t.Fatalf("got %s, want historical EVENT", typ)
			}
			queryCtx := <-started
			sendJSON(t, conn, []any{"CLOSE", "sub"})
			select {
			case <-queryCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("unfinished stream was not canceled")
			}
			// Keep the old stream open: neither receiving nor draining it may
			// prevent a new subscription on this connection from becoming live.
			sendJSON(t, conn, []any{"REQ", "sub", map[string]any{"kinds": []int{2}, "limit": 0}})
			if typ, _ := recvMessage(t, conn); typ != "EOSE" {
				t.Fatalf("got %s, want replacement EOSE", typ)
			}
			waitForRequestState(t, srv, func() bool {
				for ws, subs := range srv.listeners {
					if listener := subs["sub"]; listener != nil && len(ws.requests) == 0 {
						return len(listener.filters) == 1 && listener.filters[0].Kinds[0] == 2
					}
				}
				return false
			})
			srv.notifyListeners(&nostr.Event{ID: "live", Kind: 2})
			typ, raw := recvMessage(t, conn)
			if typ != "EVENT" {
				t.Fatalf("got %s, want live EVENT after EOSE", typ)
			}
			var event nostr.Event
			if err := json.Unmarshal(raw[2], &event); err != nil || event.ID != "live" {
				t.Fatalf("unexpected live event: %s (%v)", raw[2], err)
			}
			conn.Close()
			waitForRequestState(t, srv, func() bool { return len(srv.listeners) == 0 })
		})
	}
}

func TestCanceledHistoryWorkerExitsWithoutChannelClose(t *testing.T) {
	srv := newListenerTestServer()
	srv.options = DefaultOptions()
	srv.relay = &testRelay{}
	ws := &WebSocket{}
	req := srv.beginRequest(context.Background(), ws, "sub")
	stream := make(chan *nostr.Event)
	defer close(stream)
	started := make(chan struct{})
	storage := &testStorage{queryEvents: func(context.Context, nostr.Filter) (chan *nostr.Event, error) {
		close(started)
		return stream, nil
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.doReq(req.ctx, ws, []json.RawMessage{json.RawMessage(`"REQ"`), json.RawMessage(`"sub"`), json.RawMessage(`{"kinds":[1]}`)}, storage, req)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("history query did not start")
	}
	srv.removeListenerId(ws, "sub")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled history worker is still retaining the request")
	}
	if totalConnections(srv) != 0 || len(ws.requests) != 0 {
		t.Fatal("canceled history retained subscription state")
	}
}

// The predicate runs under the lock protecting listeners and pending requests.
func waitForRequestState(t *testing.T, srv *Server, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		srv.listenersMu.RLock()
		ok := ready()
		srv.listenersMu.RUnlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("subscription state did not settle")
}

func TestReplacementCancelsPendingHistory(t *testing.T) {
	testPendingHistoryCancellation(t, "REQ")
}

func testPendingHistoryCancellation(t *testing.T, operation string) {
	t.Helper()
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	storage := &testStorage{queryEvents: func(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
		started <- ctx
		// Keep the query in flight until the test releases it, even after
		// cancellation, to model a backend that completes late.
		<-release
		ch := make(chan *nostr.Event)
		close(ch)
		return ch, nil
	}}
	srv := startTestRelay(t, &testRelay{storage: storage})
	defer srv.Shutdown(context.Background())
	defer close(release)
	conn := dialWS(t, srv.Addr)
	sendJSON(t, conn, []any{"REQ", "sub", nostr.Filter{Kinds: []int{1}}})
	var queryCtx context.Context
	select {
	case queryCtx = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("history query did not start")
	}
	if operation == "CLOSE" {
		sendJSON(t, conn, []any{"CLOSE", "sub"})
	} else {
		sendJSON(t, conn, []any{"REQ", "sub", map[string]any{"kinds": []int{2}, "limit": 0}})
		if typ, _ := recvMessage(t, conn); typ != "EOSE" {
			t.Fatalf("replacement: got %s, want EOSE", typ)
		}
	}
	select {
	case <-queryCtx.Done():
	case <-time.After(time.Second):
		t.Fatalf("%s did not cancel the pending history query on the open connection", operation)
	}
}
