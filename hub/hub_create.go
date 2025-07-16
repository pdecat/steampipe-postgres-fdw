package hub

import (
	"log"
	"os"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/v6/logging"
)

// global hub instance
var hubSingleton Hub

// mutex protecting hub creation
var hubMux sync.Mutex

// GetHub returns a hub singleton
func GetHub() Hub {
	log.Printf("[DEBUG] Worker PID %d: GetHub() called - about to acquire hubMux lock", os.Getpid())
	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()
	log.Printf("[DEBUG] Worker PID %d: GetHub() acquired hubMux lock, returning singleton", os.Getpid())
	return hubSingleton
}

// CreateHub creates the hub
func CreateHub() error {
	log.Printf("[DEBUG] Worker PID %d: CreateHub() called - about to acquire hubMux lock", os.Getpid())
	logging.LogTime("GetHub start")

	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()
	log.Printf("[DEBUG] Worker PID %d: CreateHub() acquired hubMux lock, creating hub", os.Getpid())

	var err error
	hubSingleton, err = newRemoteHub()

	if err != nil {
		log.Printf("[ERROR] Worker PID %d: CreateHub() failed to create hub: %v", os.Getpid(), err)
		return err
	}
	log.Printf("[DEBUG] Worker PID %d: CreateHub() successfully created hub", os.Getpid())
	logging.LogTime("GetHub end")
	return err
}
