package main

/*
#include "postgres.h"
#include "common.h"

typedef struct GoFdwExecutionState
{
 uint tok;
} GoFdwExecutionState;

static inline GoFdwExecutionState* makeState(){
 GoFdwExecutionState *s = (GoFdwExecutionState *) malloc(sizeof(GoFdwExecutionState));
 return s;
}

static inline void freeState(GoFdwExecutionState * s){ if (s) free(s); }
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/turbot/steampipe-postgres-fdw/v2/hub"
	"github.com/turbot/steampipe-postgres-fdw/v2/types"
)

type ExecState struct {
	Rel   *types.Relation
	Opts  map[string]string
	Iter  hub.Iterator
	State *C.FdwExecState
}

var (
	mu   sync.RWMutex
	si   uint64
	sess = make(map[uint64]*ExecState)
)

func SaveExecState(s *ExecState) unsafe.Pointer {
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] SaveExecState - acquiring write lock for session %d", si+1))
	mu.Lock()
	si++
	i := si
	sess[i] = s
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] SaveExecState - saved session %d, total sessions: %d", i, len(sess)))
	mu.Unlock()
	cs := C.makeState()
	cs.tok = C.uint(i)
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] SaveExecState - completed for session %d", i))
	return unsafe.Pointer(cs)
}

func ClearExecState(p unsafe.Pointer) {
	if p == nil {
		FdwLogMessage(1, "[STEAMPIPE_DEBUG] ClearExecState - received nil pointer")
		return
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] ClearExecState - acquiring write lock to clear session %d", i))
	mu.Lock()
	delete(sess, i)
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] ClearExecState - cleared session %d, remaining sessions: %d", i, len(sess)))
	mu.Unlock()
	C.freeState(cs)
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] ClearExecState - completed for session %d", i))
}

func GetExecState(p unsafe.Pointer) *ExecState {
	if p == nil {
		FdwLogMessage(1, "[STEAMPIPE_DEBUG] GetExecState - received nil pointer")
		return nil
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] GetExecState - acquiring read lock for session %d", i))
	mu.RLock()
	s := sess[i]
	FdwLogMessage(1, fmt.Sprintf("[STEAMPIPE_DEBUG] GetExecState - retrieved session %d, found: %v", i, s != nil))
	mu.RUnlock()
	return s
}
