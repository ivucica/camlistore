/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package drive

import (
	"fmt"
	"log"
	"sort"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"golang.org/x/net/context"
)

type fileRecord struct {
	Title    string
	FileSize int64
}

type byTitle []*fileRecord

func (s byTitle) Len() int           { return len(s) }
func (s byTitle) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byTitle) Less(i, j int) bool { return s[i].Title < s[j].Title }

var _ blobserver.MaxEnumerateConfig = (*driveStorage)(nil)

func (sto *driveStorage) MaxEnumerate() int { return 1000 }

const firstBlobref = "sha1-0000000000000000000000000000000000000000"

func (sto *driveStorage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) (err error) {
	defer close(dest)

	// TODO: does this need to be generalized to non-sha1 digestTypes ?
	if after == "" {
		after = firstBlobref
	} else if len(after) < len(firstBlobref) {
		after = fmt.Sprintf("%s%s", after, firstBlobref[len(after):])
	}
	afterBr, ok := blob.Parse(after)
	if !ok {
		log.Printf("driveStorage.EnumerateBlobs: unparseable 'after' string: %q", after)
		afterBr, _ = blob.Parse(firstBlobref)
	}

	var chunkFiles []*fileRecord
	var pageToken string
	var chunkPrefix string

	// Find the blobref prefix length that matches at most
	// sto.MaxEnumerate() blobs.
	//
	// The camlistore enumeration API requires the blobrefs to be
	// returned in sorted order, but the Drive API doesn't give us
	// results in sorted order.  To satisfy the enumeration API,
	// we have to sort the blobrefs from Drive here before
	// returning them.
	//
	// We could slurp every single blobref into memory, but there
	// could be too many to fit in memory and the Drive API or
	// network would possibly choke on that anyway.
	//
	// We'll list successive blobref prefixes that are "bite size"
	// (<= sto.MaxEnumerate()), and then sort/return each "bite".
	for pl := sto.prefixLength; pl <= 40; pl++ {
		sto.prefixLength = pl
		chunkPrefix = afterBr.String()[:5+pl]

		chunkFiles, pageToken, err = sto.listChunk(chunkPrefix, "", sto.MaxEnumerate())
		if err != nil {
			return err
		}

		if pageToken == "" {
			// All files under this prefix fit into
			// sto.MaxEnumerate(), so we've found the
			// correct prefix length.
			break
		}
	}

	// chunkFiles has all files with the same sha1- prefix that
	// contains 'after'.  Sort chunkFiles and send only those
	// blobs that are actually after 'after'.
	var n int
	n, err = sto.sendChunk(ctx, chunkFiles, dest, after, limit)
	if err != nil {
		return err
	}
	sent := n

	// Keep using the same prefix length, and go over the
	// remaining sha1- prefixes until we've sent 'limit' blobs.
	for sent < limit {
		if chunkPrefix, ok = blob.NextPrefix(chunkPrefix); !ok {
			return nil
		}
		chunkFiles, pageToken, err = sto.listChunk(chunkPrefix, "", sto.MaxEnumerate())
		n, err = sto.sendChunk(ctx, chunkFiles, dest, after, limit-sent)
		if err != nil {
			return err
		}
		sent += n

		// Non-empty pageToken means there might be more
		// matching blobs, so keep reading.
		for pageToken != "" {
			chunkFiles, pageToken, err = sto.listChunk(chunkPrefix, pageToken, sto.MaxEnumerate())
			n, err = sto.sendChunk(ctx, chunkFiles, dest, after, limit-sent)
			if err != nil {
				return err
			}
			sent += n
		}
	}

	return nil
}

// Send at most limit SizedRefs into dest, and only those that are
// after 'after'.
func (sto *driveStorage) sendChunk(ctx context.Context, chunkFiles []*fileRecord, dest chan<- blob.SizedRef, after string, limit int) (int, error) {
	sort.Sort(byTitle(chunkFiles))
	n := 0
	for _, fileRec := range chunkFiles {
		br, ok := blob.Parse(fileRec.Title)
		if !ok || fileRec.Title <= after {
			continue
		}
		if limit <= n {
			break
		}
		select {
		case dest <- blob.SizedRef{Ref: br, Size: uint32(fileRec.FileSize)}:
			n++
		case <-ctx.Done():
			return 0, context.Canceled
		}
	}
	return n, nil
}

// List out those blobs that contain chunkPrefix in their title
func (sto *driveStorage) listChunk(chunkPrefix, pageToken string, limit int) (chunkFiles []*fileRecord, retPageToken string, err error) {
	for {
		files, pageToken, err := sto.service.List(chunkPrefix, pageToken, limit)
		if err != nil {
			return nil, "", err
		}

		for _, fi := range files {
			chunkFiles = append(chunkFiles, &fileRecord{fi.Title, fi.FileSize})
		}

		if pageToken == "" || limit < len(chunkFiles) {
			return chunkFiles, pageToken, nil
		}
	}
	return
}
