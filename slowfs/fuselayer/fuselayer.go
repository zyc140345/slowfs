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

// Package fuselayer contains the go-fuse handling code.
package fuselayer

import (
	"log"
	"os"
	"path/filepath"
	"slowfs/slowfs/scheduler"
	"slowfs/slowfs/units"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

type slowFile struct {
	nodefs.File

	path string
	sfs  *SlowFs
}

// Read performs a read, and then waits until the scheduled time.
func (sf *slowFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	start := time.Now()
	r, status := sf.File.Read(dest, off)
	// TODO(edcourtney): How long should it take in the case of an error?
	if status != fuse.OK {
		if sf.sfs.verboseLog {
			log.Printf("ERROR: Read failed for file=%s offset=%d size=%d status=%s", 
				sf.path, off, len(dest), status)
		}
		return r, status
	}

	// The read doesn't actually get executed until we do it explicitly, so do it now.
	// If we don't, time will get spent doing the read where we don't expect.
	buf := make([]byte, r.Size())
	buf, status = r.Bytes(buf)
	// TODO(edcourtney): How long should it take in the case of an error?
	if status != fuse.OK {
		return nil, status
	}
	r = fuse.ReadResultData(buf)

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.ReadRequest,
		Timestamp: start,
		Path:      sf.path,
		Start:     units.NumBytes(off),
		Size:      units.NumBytes(r.Size()),
	})

	time.Sleep(opTime - time.Since(start))

	return r, status
}

// Write performs a write, and then waits until the scheduled time.
func (sf *slowFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	start := time.Now()
	// Unlike Read, Write will immediately execute the syscall.
	r, status := sf.File.Write(data, off)

	// TODO(edcourtney): How long should it take in the case of an error?
	if status != fuse.OK {
		if sf.sfs.verboseLog {
			log.Printf("ERROR: Write failed for file=%s offset=%d size=%d status=%s", 
				sf.path, off, len(data), status)
		}
		return r, status
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.WriteRequest,
		Timestamp: start,
		Path:      sf.path,
		Start:     units.NumBytes(off),
		Size:      units.NumBytes(r),
	})

	time.Sleep(opTime - time.Since(start))

	return r, status
}

// Release calls Release on the underlying file, and then waits until the scheduled time.
func (sf *slowFile) Release() {
	start := time.Now()
	sf.File.Release()

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.CloseRequest,
		Timestamp: start,
		Path:      sf.path,
	})
	time.Sleep(opTime - time.Since(start))
}

func (sf *slowFile) Fsync(flags int) fuse.Status {
	start := time.Now()
	r := sf.File.Fsync(flags)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.FsyncRequest,
		Timestamp: start,
		Path:      sf.path,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) Truncate(size uint64) fuse.Status {
	start := time.Now()
	r := sf.File.Truncate(size)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) GetAttr(out *fuse.Attr) fuse.Status {
	start := time.Now()
	r := sf.File.GetAttr(out)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	// Only override if this is the root directory (path is empty or root)
	if (sf.path == "" || sf.path == "/") && sf.sfs.uid != 0 && sf.sfs.gid != 0 {
		// This is the root directory, override with original ownership
		out.Uid = sf.sfs.uid
		out.Gid = sf.sfs.gid
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) Chown(uid uint32, gid uint32) fuse.Status {
	start := time.Now()
	r := sf.File.Chown(uid, gid)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) Chmod(perms uint32) fuse.Status {
	start := time.Now()
	r := sf.File.Chmod(perms)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	start := time.Now()
	r := sf.File.Utimens(atime, mtime)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

func (sf *slowFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	start := time.Now()
	r := sf.File.Allocate(off, size, mode)
	// TODO(edcourtney): How long should this take?
	if r != fuse.OK {
		return r
	}

	opTime := sf.sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.AllocateRequest,
		Timestamp: start,
		Size:      units.NumBytes(size),
	})
	time.Sleep(opTime - time.Since(start))

	return r
}

// SlowFs is a FileSystem whose operations take amounts of time determined by an associated
// Scheduler.
type SlowFs struct {
	pathfs.FileSystem

	scheduler  *scheduler.Scheduler
	uid        uint32
	gid        uint32
	rootPath   string
	verboseLog bool
}

// NewSlowFs creates a new SlowFs using the specified scheduler at the given directory. The
// directory must be empty.
func NewSlowFs(directory string, scheduler *scheduler.Scheduler) *SlowFs {
	return &SlowFs{
		FileSystem: pathfs.NewLoopbackFileSystem(directory),
		scheduler:  scheduler,
		uid:        0,
		gid:        0,
		rootPath:   directory,
	}
}

// NewSlowFsWithOwner creates a new SlowFs with specific uid/gid
func NewSlowFsWithOwner(directory string, scheduler *scheduler.Scheduler, uid, gid uint32, verboseLog bool) *SlowFs {
	return &SlowFs{
		FileSystem: pathfs.NewLoopbackFileSystem(directory),
		scheduler:  scheduler,
		uid:        uid,
		gid:        gid,
		rootPath:   directory,
		verboseLog: verboseLog,
	}
}

// Open opens a file, and then waits until the scheduled time.
func (sfs *SlowFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	start := time.Now()
	
	// Log file access with user context (only in verbose mode)
	if sfs.verboseLog && context != nil {
		log.Printf("OPEN: uid=%d gid=%d file=%s flags=0x%x", 
			context.Caller.Uid, context.Caller.Gid, name, flags)
	}
	
	// Check if this is a create operation
	fileExists := true
	if _, err := os.Stat(filepath.Join(sfs.rootPath, name)); os.IsNotExist(err) {
		fileExists = false
	}
	
	file, status := sfs.FileSystem.Open(name, flags, context)
	// TODO(edcourtney): How long should it take in the case of an error?
	if status != fuse.OK {
		if sfs.verboseLog && context != nil {
			log.Printf("ERROR: Open failed for uid=%d file=%s status=%s", 
				context.Caller.Uid, name, status)
		}
		return file, status
	}

	// If file was created and we have context, set correct ownership
	if !fileExists && (flags&syscall.O_CREAT != 0) && context != nil {
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		fullPath := filepath.Join(sfs.rootPath, name)
		if err := syscall.Chown(fullPath, int(targetUid), int(targetGid)); err != nil {
			log.Printf("Warning: failed to set ownership of opened/created file %s: %v", fullPath, err)
		}
	}

	slowFile := &slowFile{
		File: file,
		sfs:  sfs,
		path: name,
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return slowFile, status
}

// GetAttr calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	start := time.Now()
	attr, status := sfs.FileSystem.GetAttr(name, context)
	if status != fuse.OK {
		return attr, status
	}

	// Only override root directory uid/gid, other files should have correct ownership
	if name == "" && sfs.uid != 0 && sfs.gid != 0 {
		// This is the root directory, override with original ownership
		attr.Uid = sfs.uid
		attr.Gid = sfs.gid
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return attr, status
}

// Chmod calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Chmod(name, mode, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Chown calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Chown(name, uid, gid, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Utimens calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Utimens(name, Atime, Mtime, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Truncate calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Truncate(name string, size uint64, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Truncate(name, size, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Access calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Access(name, mode, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Link calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Link(oldName string, newName string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Link(oldName, newName, context)
	if status != fuse.OK {
		return status
	}

	// Set correct ownership if context is available
	if context != nil {
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		fullPath := filepath.Join(sfs.rootPath, newName)
		if err := syscall.Chown(fullPath, int(targetUid), int(targetGid)); err != nil {
			log.Printf("Warning: failed to set ownership of linked file %s: %v", fullPath, err)
		}
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Mkdir calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Mkdir(name, mode, context)
	if status != fuse.OK {
		if context != nil {
			log.Printf("ERROR: Mkdir failed for uid=%d dir=%s status=%s", 
				context.Caller.Uid, name, status)
		}
		return status
	}

	// Set correct ownership if context is available
	if context != nil {
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		fullPath := filepath.Join(sfs.rootPath, name)
		if err := syscall.Chown(fullPath, int(targetUid), int(targetGid)); err != nil {
			log.Printf("Warning: failed to set ownership of created directory %s: %v", fullPath, err)
		}
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Mknod calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Mknod(name, mode, dev, context)
	if status != fuse.OK {
		return status
	}

	// Set correct ownership if context is available
	if context != nil {
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		fullPath := filepath.Join(sfs.rootPath, name)
		if err := syscall.Chown(fullPath, int(targetUid), int(targetGid)); err != nil {
			log.Printf("Warning: failed to set ownership of created node %s: %v", fullPath, err)
		}
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Rename calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Rename(oldName, newName, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Rmdir calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Rmdir(name string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Rmdir(name, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Unlink calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Unlink(name string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Unlink(name, context)
	if status != fuse.OK {
		if context != nil {
			log.Printf("ERROR: Unlink failed for uid=%d file=%s status=%s", 
				context.Caller.Uid, name, status)
		}
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// GetXAttr calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	start := time.Now()
	data, status := sfs.FileSystem.GetXAttr(name, attribute, context)
	if status != fuse.OK {
		return data, status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return data, status
}

// ListXAttr calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	start := time.Now()
	attributes, status := sfs.FileSystem.ListXAttr(name, context)
	if status != fuse.OK {
		return attributes, status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return attributes, status
}

// RemoveXAttr calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.RemoveXAttr(name, attr, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// SetXAttr calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.SetXAttr(name, attr, data, flags, context)
	if status != fuse.OK {
		return status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Create calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	start := time.Now()
	file, status := sfs.FileSystem.Create(name, flags, mode, context)
	if status != fuse.OK {
		if context != nil {
			log.Printf("ERROR: Create failed for uid=%d file=%s status=%s", 
				context.Caller.Uid, name, status)
		}
		return file, status
	}

	// Set correct ownership if context is available
	if context != nil {
		// Use the FUSE context's uid/gid 
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		// Construct the full path to the created file
		fullPath := filepath.Join(sfs.rootPath, name)
		if err := syscall.Chown(fullPath, int(targetUid), int(targetGid)); err != nil {
			// Log the error but don't fail the operation
			log.Printf("Warning: failed to set ownership of created file %s: %v", fullPath, err)
		}
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return file, status
}

// OpenDir calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	start := time.Now()
	stream, status := sfs.FileSystem.OpenDir(name, context)
	if status != fuse.OK {
		return stream, status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return stream, status
}

// Symlink calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Symlink(value string, linkName string, context *fuse.Context) fuse.Status {
	start := time.Now()
	status := sfs.FileSystem.Symlink(value, linkName, context)
	if status != fuse.OK {
		return status
	}

	// Set correct ownership if context is available
	if context != nil {
		targetUid := context.Caller.Uid
		targetGid := context.Caller.Gid
		
		fullPath := filepath.Join(sfs.rootPath, linkName)
		if err := syscall.Lchown(fullPath, int(targetUid), int(targetGid)); err != nil {
			log.Printf("Warning: failed to set ownership of symlink %s: %v", fullPath, err)
		}
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return status
}

// Readlink calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	start := time.Now()
	f, status := sfs.FileSystem.Readlink(name, context)
	if status != fuse.OK {
		return f, status
	}

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return f, status
}

// StatFs calls the underlying filesystem then sends a MetadataRequest and
// waits how long it is told to.
func (sfs *SlowFs) StatFs(name string) *fuse.StatfsOut {
	start := time.Now()
	out := sfs.FileSystem.StatFs(name)

	opTime := sfs.scheduler.Schedule(&scheduler.Request{
		Type:      scheduler.MetadataRequest,
		Timestamp: start,
	})
	time.Sleep(opTime - time.Since(start))

	return out
}
