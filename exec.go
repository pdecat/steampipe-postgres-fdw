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
	"log"
	"os"
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
	log.Printf("[DEBUG] Worker PID %d: SaveExecState - acquiring write lock for session %d", os.Getpid(), si+1)
	mu.Lock()
	si++
	i := si
	sess[i] = s
	log.Printf("[DEBUG] Worker PID %d: SaveExecState - saved session %d, total sessions: %d", os.Getpid(), i, len(sess))
	mu.Unlock()
	cs := C.makeState()
	cs.tok = C.uint(i)
	log.Printf("[DEBUG] Worker PID %d: SaveExecState - completed for session %d", os.Getpid(), i)
	return unsafe.Pointer(cs)
}

func ClearExecState(p unsafe.Pointer) {
	if p == nil {
		log.Printf("[DEBUG] Worker PID %d: ClearExecState - received nil pointer", os.Getpid())
		return
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	log.Printf("[DEBUG] Worker PID %d: ClearExecState - acquiring write lock to clear session %d", os.Getpid(), i)
	mu.Lock()
	delete(sess, i)
	log.Printf("[DEBUG] Worker PID %d: ClearExecState - cleared session %d, remaining sessions: %d", os.Getpid(), i, len(sess))
	mu.Unlock()
	C.freeState(cs)
	log.Printf("[DEBUG] Worker PID %d: ClearExecState - completed for session %d", os.Getpid(), i)
}

func GetExecState(p unsafe.Pointer) *ExecState {
	if p == nil {
		log.Printf("[DEBUG] Worker PID %d: GetExecState - received nil pointer", os.Getpid())
		return nil
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	log.Printf("[DEBUG] Worker PID %d: GetExecState - acquiring read lock for session %d", os.Getpid(), i)
	mu.RLock()
	s := sess[i]
	log.Printf("[DEBUG] Worker PID %d: GetExecState - retrieved session %d, found: %v", os.Getpid(), i, s != nil)
	mu.RUnlock()
	return s
}
