// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import "sync"

// deliveredSet is a bounded, insertion-ordered set of update IDs. When full, the
// oldest ID is evicted.
type deliveredSet struct {
	mutex    sync.Mutex
	ids      map[string]struct{}
	order    []string
	next     int
	capacity int
}

func newDeliveredSet(capacity int) *deliveredSet {
	return &deliveredSet{
		ids:      make(map[string]struct{}, capacity),
		order:    make([]string, capacity),
		capacity: capacity,
	}
}

// add records id and reports whether it was new. An empty ID cannot be
// deduplicated and is always reported as new.
func (s *deliveredSet) add(id string) bool {
	if id == "" {
		return true
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, seen := s.ids[id]; seen {
		return false
	}

	if evicted := s.order[s.next]; evicted != "" {
		delete(s.ids, evicted)
	}

	s.order[s.next] = id
	s.next = (s.next + 1) % s.capacity
	s.ids[id] = struct{}{}

	return true
}
