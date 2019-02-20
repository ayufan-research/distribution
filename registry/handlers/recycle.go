package handlers

import (
	"net/http"

	"github.com/docker/distribution/context"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// blobDispatcher uses the request context to build a blobHandler.
func recycleDispatcher(ctx *Context, r *http.Request) http.Handler {
	recycleHandler := &recycleHandler{
		Context: ctx,
	}

	mhandler := handlers.MethodHandler{}

	if !ctx.readOnly {
		mhandler["DELETE"] = http.HandlerFunc(recycleHandler.Recycle)
	}

	return mhandler
}

// blobHandler serves http blob requests.
type recycleHandler struct {
	*Context

	Digest digest.Digest
}

// GetBlob fetches the binary data from backend storage returns it in the
// response.
func (bh *blobHandler) Recycle(w http.ResponseWriter, r *http.Request) {
	context.GetLogger(bh).Debug("Recycle")

	// blobs := bh.Repository.Blobs(bh)
	// desc, err := blobs.Stat(bh, bh.Digest)
	// if err != nil {
	// 	if err == distribution.ErrBlobUnknown {
	// 		bh.Errors = append(bh.Errors, v2.ErrorCodeBlobUnknown.WithDetail(bh.Digest))
	// 	} else {
	// 		bh.Errors = append(bh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
	// 	}
	// 	return
	// }

	// if err := blobs.ServeBlob(bh, w, r, desc.Digest); err != nil {
	// 	context.GetLogger(bh).Debugf("unexpected error getting blob HTTP handler: %v", err)
	// 	bh.Errors = append(bh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
	// 	return
	// }

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
}
