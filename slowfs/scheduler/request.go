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

package scheduler

import (
	"slowfs/slowfs/units"
	"time"
)

// RequestType denotes what type a request is.
type RequestType int64

// Enumeration of different types of requests.
const (
	ReadRequest RequestType = iota
	WriteRequest
	OpenRequest
	CloseRequest
	FsyncRequest
	AllocateRequest
	MetadataRequest
)

// String returns the string representation of RequestType
func (rt RequestType) String() string {
	switch rt {
	case ReadRequest:
		return "READ"
	case WriteRequest:
		return "WRITE"
	case OpenRequest:
		return "OPEN"
	case CloseRequest:
		return "CLOSE"
	case FsyncRequest:
		return "FSYNC"
	case AllocateRequest:
		return "ALLOCATE"
	case MetadataRequest:
		return "METADATA"
	default:
		return "UNKNOWN"
	}
}

// Request contains information for all types of requests.
type Request struct {
	Type      RequestType
	Timestamp time.Time
	Path      string
	Start     units.NumBytes
	Size      units.NumBytes
}
