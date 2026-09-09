package relayer

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// Only in-flight requests are tracked. Removing an entry invalidates its
// registration token, without retaining a tombstone for every closed ID.
type subscriptionRequest struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Server) beginRequest(ctx context.Context, ws *WebSocket, id string) *subscriptionRequest {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	req := &subscriptionRequest{ctx: ctx, cancel: cancel}
	if ws.disconnected {
		cancel()
		return req
	}
	if old := ws.requests[id]; old != nil {
		old.cancel()
	}
	if ws.requests == nil {
		ws.requests = make(map[string]*subscriptionRequest)
	}
	ws.requests[id] = req
	// A replacement stops the old live subscription immediately as well.
	s.removeListenerIdLocked(ws, id)
	return req
}

func (s *Server) finishRequest(ws *WebSocket, id string, req *subscriptionRequest) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	if ws.requests[id] == req {
		delete(ws.requests, id)
		if len(ws.requests) == 0 {
			ws.requests = nil
		}
	}
	req.cancel()
}

func (s *Server) registerRequest(ws *WebSocket, id string, req *subscriptionRequest, filters nostr.Filters) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	if ws.requests[id] != req || req.ctx.Err() != nil {
		return
	}
	s.setListenerLocked(id, ws, filters)
}

type Listener struct {
	filters nostr.Filters
}

func GetListeningFilters() nostr.Filters {
	serversMutex.RLock()
	defer serversMutex.RUnlock()

	respfilters := make(nostr.Filters, 0, len(servers)*2)
	for srv := range servers {
		respfilters = appendDistinctFilters(respfilters, srv.GetListeningFilters())
	}

	return respfilters
}

func (s *Server) GetListeningFilters() nostr.Filters {
	s.listenersMu.RLock()
	defer s.listenersMu.RUnlock()
	respfilters := make(nostr.Filters, 0, len(s.listeners)*2)

	for _, connlisteners := range s.listeners {
		for _, listener := range connlisteners {
			respfilters = appendDistinctFilters(respfilters, listener.filters)
		}
	}

	return respfilters
}

func appendDistinctFilters(dst nostr.Filters, src nostr.Filters) nostr.Filters {
	for _, listenerfilter := range src {
		duplicate := false
		for _, respfilter := range dst {
			if nostr.FilterEqual(listenerfilter, respfilter) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			dst = append(dst, listenerfilter)
		}
	}
	return dst
}

func (s *Server) setListener(id string, ws *WebSocket, filters nostr.Filters) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	s.setListenerLocked(id, ws, filters)
}

func (s *Server) setListenerLocked(id string, ws *WebSocket, filters nostr.Filters) {
	if ws.disconnected {
		return
	}

	subs, ok := s.listeners[ws]
	if !ok {
		subs = make(map[string]*Listener)
		s.listeners[ws] = subs
	}

	subs[id] = &Listener{filters: filters}
}

// Remove a specific subscription id from listeners for a given ws client
func (s *Server) removeListenerId(ws *WebSocket, id string) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	if req := ws.requests[id]; req != nil {
		req.cancel()
		delete(ws.requests, id)
		if len(ws.requests) == 0 {
			ws.requests = nil
		}
	}
	s.removeListenerIdLocked(ws, id)
}

func (s *Server) removeListenerIdLocked(ws *WebSocket, id string) {
	if subs, ok := s.listeners[ws]; ok {
		delete(s.listeners[ws], id)
		if len(subs) == 0 {
			delete(s.listeners, ws)
		}
	}
}

// Remove WebSocket conn from listeners
func (s *Server) removeListener(ws *WebSocket) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	ws.disconnected = true
	for _, req := range ws.requests {
		req.cancel()
	}
	ws.requests = nil
	clear(s.listeners[ws])
	delete(s.listeners, ws)
}

type listenerDelivery struct {
	ws    *WebSocket
	subID string
	event nostr.Event
}

func (s *Server) notifyListeners(event *nostr.Event) {
	s.listenersMu.RLock()
	deliveries := make([]listenerDelivery, 0, len(s.listeners))
	for ws, subs := range s.listeners {
		for id, listener := range subs {
			if !listener.filters.Match(event) {
				continue
			}
			deliveries = append(deliveries, listenerDelivery{
				ws:    ws,
				subID: id,
				event: *event,
			})
		}
	}
	s.listenersMu.RUnlock()

	for _, delivery := range deliveries {
		delivery := delivery
		delivery.ws.WriteJSON(nostr.EventEnvelope{SubscriptionID: &delivery.subID, Event: delivery.event})
	}
}

func BroadcastEvent(evt *nostr.Event) {
	serversMutex.RLock()
	defer serversMutex.RUnlock()

	for srv := range servers {
		srv.notifyListeners(evt)
	}
}
