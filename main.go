// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slowfs/slowfs"
	"slowfs/slowfs/fuselayer"
	"slowfs/slowfs/scheduler"
	"slowfs/slowfs/units"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// getDirectoryOwner returns the uid and gid of the given directory
func getDirectoryOwner(dirPath string) (uint32, uint32, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(dirPath, &stat); err != nil {
		return 0, 0, fmt.Errorf("failed to stat directory %s: %v", dirPath, err)
	}
	return stat.Uid, stat.Gid, nil
}

// moveToSecureLocation moves the backing directory to a secure location
// and returns the new path
func moveToSecureLocation(originalPath string) (string, error) {
	// Check if we're running as root
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("secure mode requires root privileges")
	}

	// Create secure directory if it doesn't exist
	secureBaseDir := "/home/.slowfs"
	if err := os.MkdirAll(secureBaseDir, 0700); err != nil {
		return "", fmt.Errorf("failed to create secure base directory: %v", err)
	}
	
	// Set ownership to root
	if err := os.Chown(secureBaseDir, 0, 0); err != nil {
		return "", fmt.Errorf("failed to set secure base directory ownership: %v", err)
	}

	// Generate secure path
	baseName := filepath.Base(originalPath)
	securePath := filepath.Join(secureBaseDir, baseName)
	
	// Check if secure path already exists
	if _, err := os.Stat(securePath); err == nil {
		return "", fmt.Errorf("secure path %s already exists", securePath)
	}

	// Move directory
	if err := os.Rename(originalPath, securePath); err != nil {
		return "", fmt.Errorf("failed to move directory to secure location: %v", err)
	}

	fmt.Printf("Moved backing directory from %s to %s\n", originalPath, securePath)
	return securePath, nil
}

// restoreFromSecureLocation moves the directory back to its original location
func restoreFromSecureLocation(securePath, originalPath string) error {
	// Check if secure path exists
	if _, err := os.Stat(securePath); os.IsNotExist(err) {
		return fmt.Errorf("secure path %s does not exist", securePath)
	}

	// Check if original path already exists
	if _, err := os.Stat(originalPath); err == nil {
		return fmt.Errorf("original path %s already exists", originalPath)
	}

	// Move directory back
	if err := os.Rename(securePath, originalPath); err != nil {
		return fmt.Errorf("failed to restore directory: %v", err)
	}

	fmt.Printf("Restored backing directory from %s to %s\n", securePath, originalPath)
	return nil
}

// forceUnmount attempts to forcefully unmount by killing processes using the mount point
func forceUnmount(mountPath string) error {
	fmt.Printf("Attempting to force unmount %s by killing processes...\n", mountPath)
	
	// First try to kill processes using fuser
	cmd := exec.Command("fuser", "-km", mountPath)
	if err := cmd.Run(); err != nil {
		log.Printf("fuser command failed: %v", err)
	}
	
	// Wait a moment for processes to terminate
	time.Sleep(2 * time.Second)
	
	// Try lazy unmount as last resort
	cmd = exec.Command("umount", "-l", mountPath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("lazy unmount failed: %v", err)
	}
	
	fmt.Printf("Successfully force unmounted %s\n", mountPath)
	return nil
}

// cleanup handles cleanup operations when the program exits
func cleanup(server *fuse.Server, securePath, originalPath, mountPath string, enableSecureMode bool) {
	fmt.Println("Cleaning up...")
	
	// Unmount filesystem with retry mechanism
	if server != nil {
		err := server.Unmount()
		if err != nil {
			log.Printf("Normal unmount failed: %v", err)
			
			// Try force unmount
			if forceErr := forceUnmount(mountPath); forceErr != nil {
				log.Printf("ERROR: Force unmount also failed: %v", forceErr)
			} else {
				log.Printf("Filesystem forcefully unmounted")
			}
		} else {
			fmt.Println("Filesystem unmounted successfully")
		}
	}

	// Restore directory if in secure mode
	if enableSecureMode && securePath != "" && originalPath != "" {
		// If mount point was the same as original path, remove the empty mount directory first
		if originalPath == mountPath {
			if err := os.Remove(mountPath); err != nil {
				log.Printf("Warning: failed to remove mount point directory: %v", err)
			}
		}
		
		if err := restoreFromSecureLocation(securePath, originalPath); err != nil {
			log.Printf("Error restoring directory: %v", err)
		}
	}
}

func main() {
	configs := map[string]*slowfs.DeviceConfig{
		slowfs.HDD7200RpmDeviceConfig.Name: &slowfs.HDD7200RpmDeviceConfig,
	}

	backingDir := flag.String("backing-dir", "", "directory to use as storage")
	mountDir := flag.String("mount-dir", "", "directory to mount at")
	secureMode := flag.Bool("secure-mode", false, "enable secure mode (moves backing directory to prevent bypass)")

	configFile := flag.String("config-file", "", "path to config file listing device configurations")
	configName := flag.String("config-name", "hdd7200rpm", "which config to use (built-ins: hdd7200rpm)")
	verboseLog := flag.Bool("verbose", false, "enable verbose logging for debugging")

	// Flags for overriding any subset of the config. These are all strings (even the durations)
	// because we need to differentiate between the flag not being specified, and being set to the
	// default value.
	seekWindow := flag.String("seek-window", "", "")
	seekTime := flag.String("seek-time", "", "")
	readBytesPerSecond := flag.String("read-bytes-per-second", "", "")
	writeBytesPerSecond := flag.String("write-bytes-per-second", "", "")
	allocateBytesPerSecond := flag.String("allocate-bytes-per-second", "", "")
	requestReorderMaxDelay := flag.String("request-reorder-max-delay", "", "")
	fsyncStrategy := flag.String("fsync-strategy", "", "choice of none/no, dumb, writebackcache/wbc")
	writeStrategy := flag.String("write-strategy", "", "choice of fast, simulate")
	metadataOpTime := flag.String("metadata-op-time", "", "duration value (e.g. 10ms)")
	flag.Parse()

	if *backingDir == "" || *mountDir == "" {
		log.Fatalf("arguments backing-dir and mount-dir are required.")
	}

	var err error

	*backingDir, err = filepath.Abs(*backingDir)
	if err != nil {
		log.Fatalf("invalid backing-dir: %v", err)
	}

	*mountDir, err = filepath.Abs(*mountDir)
	if err != nil {
		log.Fatalf("invalid mount-dir: %v", err)
	}

	// In secure mode, we allow backing-dir and mount-dir to be the same
	// because we'll move the backing directory to a secure location
	if *backingDir == *mountDir && !*secureMode {
		log.Fatalf("backing directory may not be the same as mount directory (unless using --secure-mode)")
	}

	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("couldn't read config file %s: %s", *configFile, err)
		}
		dcs, err := slowfs.ParseDeviceConfigsFromJSON(data)
		if err != nil {
			log.Fatalf("couldn't parse config file %s: %s", *configFile, err)
		}
		for _, dc := range dcs {
			if _, ok := configs[dc.Name]; ok {
				log.Fatalf("duplicate device config with name '%s'", dc.Name)
			}
			configs[dc.Name] = dc
		}
	}

	config, ok := configs[*configName]

	if !ok {
		log.Fatalf("unknown config %s", *configName)
	}

	flagsHadError := false

	if *seekWindow != "" {
		config.SeekWindow, err = units.ParseNumBytesFromString(*seekWindow)
		if err != nil {
			log.Printf("flag seek-window: %s", err)
			flagsHadError = true
		}
	}

	if *seekTime != "" {
		config.SeekTime, err = time.ParseDuration(*seekTime)
		if err != nil {
			log.Printf("flag seek-time: %s", err)
			flagsHadError = true
		}
	}

	if *readBytesPerSecond != "" {
		config.ReadBytesPerSecond, err = units.ParseNumBytesFromString(*readBytesPerSecond)
		if err != nil {
			log.Printf("flag read-bytes-per-second: %s", err)
			flagsHadError = true
		}
	}

	if *writeBytesPerSecond != "" {
		config.WriteBytesPerSecond, err = units.ParseNumBytesFromString(*writeBytesPerSecond)
		if err != nil {
			log.Printf("flag write-bytes-per-second: %s", err)
			flagsHadError = true
		}
	}

	if *allocateBytesPerSecond != "" {
		config.AllocateBytesPerSecond, err = units.ParseNumBytesFromString(*allocateBytesPerSecond)
		if err != nil {
			log.Printf("flag allocate-bytes-per-second: %s", err)
			flagsHadError = true
		}
	}

	if *requestReorderMaxDelay != "" {
		config.RequestReorderMaxDelay, err = time.ParseDuration(*requestReorderMaxDelay)
		if err != nil {
			log.Printf("flag request-reorder-max-delay: %s", err)
			flagsHadError = true
		}
	}

	if *fsyncStrategy != "" {
		config.FsyncStrategy, err = slowfs.ParseFsyncStrategyFromString(*fsyncStrategy)
		if err != nil {
			log.Printf("flag fsync-strategy: %s", err)
			flagsHadError = true
		}
	}

	if *writeStrategy != "" {
		config.WriteStrategy, err = slowfs.ParseWriteStrategyFromString(*writeStrategy)
		if err != nil {
			log.Printf("flag write-strategy: %s", err)
			flagsHadError = true
		}
	}

	if *metadataOpTime != "" {
		config.MetadataOpTime, err = time.ParseDuration(*metadataOpTime)
		if err != nil {
			log.Printf("flag metadata-op-time: %s", err)
			flagsHadError = true
		}
	}

	if flagsHadError {
		log.Fatalf("flags had error(s), exiting")
	}

	err = config.Validate()
	if err != nil {
		log.Fatalf("error validating config: %s", err)
	}

	fmt.Printf("using config: %s\n", config)
	
	// Store original backing directory path for cleanup
	originalBackingDir := *backingDir
	var secureBackingDir string
	
	// Handle secure mode
	if *secureMode {
		fmt.Println("Secure mode enabled")
		secureBackingDir, err = moveToSecureLocation(*backingDir)
		if err != nil {
			log.Fatalf("failed to move directory to secure location: %v", err)
		}
		
		// If mount-dir and original backing-dir were the same, we need to create the mount point
		if originalBackingDir == *mountDir {
			if err := os.MkdirAll(*mountDir, 0755); err != nil {
				// Try to restore on error
				if restoreErr := restoreFromSecureLocation(secureBackingDir, originalBackingDir); restoreErr != nil {
					log.Printf("Failed to restore directory after mkdir error: %v", restoreErr)
				}
				log.Fatalf("failed to create mount point directory: %v", err)
			}
			fmt.Printf("Created mount point directory: %s\n", *mountDir)
		}
		
		*backingDir = secureBackingDir
	}
	
	// Get the owner of the backing directory
	uid, gid, err := getDirectoryOwner(*backingDir)
	if err != nil {
		log.Fatalf("failed to get backing directory owner: %v", err)
	}
	fmt.Printf("Detected backing directory owner: uid=%d, gid=%d\n", uid, gid)
	
	scheduler := scheduler.New(config)
	fs := pathfs.NewPathNodeFs(fuselayer.NewSlowFsWithOwner(*backingDir, scheduler, uid, gid, *verboseLog), nil)
	
	// Create mount options with proper uid/gid mapping
	mountOpts := &fuse.MountOptions{
		AllowOther: true,
		Options: []string{
			"default_permissions",
		},
	}
	
	nodefsOpts := &nodefs.Options{}
	
	server, _, err := nodefs.Mount(*mountDir, fs.Root(), mountOpts, nodefsOpts)
	if err != nil {
		// If mount fails and we're in secure mode, restore the directory
		if *secureMode && secureBackingDir != "" {
			if restoreErr := restoreFromSecureLocation(secureBackingDir, originalBackingDir); restoreErr != nil {
				log.Printf("Failed to restore directory after mount error: %v", restoreErr)
			}
		}
		log.Fatalf("%v", err)
	}

	fmt.Printf("Mounted %s at %s with uid=%d, gid=%d\n", *backingDir, *mountDir, uid, gid)
	log.Printf("SlowFS started: backing=%s mount=%s config=%s secure=%v", *backingDir, *mountDir, *configName, *secureMode)
	
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Handle cleanup in a separate goroutine
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating shutdown...", sig)
		cleanup(server, secureBackingDir, originalBackingDir, *mountDir, *secureMode)
		log.Printf("SlowFS shutdown completed")
		os.Exit(0)
	}()
	
	// Serve the filesystem
	server.Serve()
	
	// If we reach here, server.Serve() returned, so clean up
	cleanup(server, secureBackingDir, originalBackingDir, *mountDir, *secureMode)
}
