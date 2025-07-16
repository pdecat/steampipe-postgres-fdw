package settings

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

func (s *HubCacheSettings) SetEnabled(jsonValue string) error {
	log.Printf("[TRACE] Worker PID %d: SetEnabled %s", os.Getpid(), jsonValue)
	var enable bool
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	s.ClientCacheEnabled = &enable
	return nil
}

func (s *HubCacheSettings) SetTtl(jsonValue string) error {
	log.Printf("[TRACE] Worker PID %d: SetTtl %s", os.Getpid(), jsonValue)
	var enable int
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	ttl := time.Duration(enable) * time.Second
	s.Ttl = &ttl
	return nil
}

func (s *HubCacheSettings) SetClearTime(_ string) error {
	log.Printf("[TRACE] Worker PID %d: SetClearTime", os.Getpid())
	s.ClearTime = time.Now()
	return nil
}
