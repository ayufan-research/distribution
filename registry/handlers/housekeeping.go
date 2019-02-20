package handlers

import (
	"fmt"
	"net/http"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// blobDispatcher uses the request context to build a blobHandler.
func housekeepingDispatcher(ctx *Context, r *http.Request) http.Handler {
	housekeepingHandler := &housekeepingHandler{
		Context: ctx,
	}

	mhandler := handlers.MethodHandler{}

	if !ctx.readOnly {
		mhandler["DELETE"] = http.HandlerFunc(housekeepingHandler.Recycle)
	}

	return mhandler
}

// blobHandler serves http blob requests.
type housekeepingHandler struct {
	*Context

	Digest digest.Digest
}

type ManifestDel struct {
	Name   string
	Digest digest.Digest
	Tags   []string
}

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

func (bh *housekeepingHandler) markAllManifests(service distribution.ManifestService, manifestArr *[]ManifestDel, markSet map[digest.Digest]struct{}) error {
	manifestEnumerator, ok := service.(distribution.ManifestEnumerator)
	if !ok {
		return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
	}

	removeUntagged := true

	err := manifestEnumerator.Enumerate(bh.Context, func(dgst digest.Digest) error {
		if removeUntagged {
			// fetch all tags where this manifest is the latest one
			tags, err := bh.Repository.Tags(bh.Context).Lookup(bh.Context, distribution.Descriptor{Digest: dgst})
			if err != nil {
				return fmt.Errorf("failed to retrieve tags for digest %v: %v", dgst, err)
			}

			if len(tags) == 0 {
				emit("manifest eligible for deletion: %s", dgst)
				// fetch all tags from repository
				// all of these tags could contain manifest in history
				// which means that we need check (and delete) those references when deleting manifest
				allTags, err := bh.Repository.Tags(bh.Context).All(bh.Context)
				if err != nil {
					return fmt.Errorf("failed to retrieve tags %v", err)
				}

				*manifestArr = append(*manifestArr, ManifestDel{Name: bh.Repository.Named().Name(), Digest: dgst, Tags: allTags})
				return nil
			}
		}

		// Mark the manifest's blob
		emit("%s: marking manifest %s ", bh.Repository.Named().Name(), dgst)
		markSet[dgst] = struct{}{}

		manifest, err := service.Get(bh.Context, dgst)
		if err != nil {
			return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
		}

		descriptors := manifest.References()
		for _, descriptor := range descriptors {
			markSet[descriptor.Digest] = struct{}{}
			emit("%s: marking blob %s", bh.Repository.Named().Name(), descriptor.Digest)
		}

		return nil
	})

	return err
}

func (bh *housekeepingHandler) runGCCycle() error {
	manifestService, err := bh.Repository.Manifests(bh.Context)
	if err != nil {
		return fmt.Errorf("failed to construct manifest service: %v", err)
	}

	blobsService := bh.Repository.RepositoryBlobsEnumerator(bh.Context)

	markSet := make(map[digest.Digest]struct{})
	manifestArr := make([]ManifestDel, 0)
	err = bh.markAllManifests(manifestService, &manifestArr, markSet)
	if err != nil {
		return fmt.Errorf("failed to mark all manifests: %v", err)
	}

	vacuum := storage.NewVacuum(bh.Context, bh.driver)

	for _, obj := range manifestArr {
		err = vacuum.RemoveManifest(obj.Name, obj.Digest, obj.Tags)
		if err != nil {
			return fmt.Errorf("failed to delete manifest %s: %v", obj.Digest, err)
		}
	}

	// remove blobs only from our repository
	if blobsService != nil && blobsService.IsScopped() {
		err = blobsService.Enumerate(bh.Context, func(dgst digest.Digest) error {
			// check if digest is in markSet. If not, delete it!
			if _, ok := markSet[dgst]; !ok {
				vacuum.RemoveRepositoryBlob(bh.Repository.Named().Name(), dgst)
			}
			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to delete blobs %v", err)
		}
	}

	return nil
}

// GetBlob fetches the binary data from backend storage returns it in the
// response.
func (bh *housekeepingHandler) Recycle(w http.ResponseWriter, r *http.Request) {
	context.GetLogger(bh).Debug("Recycle")

	err := bh.runGCCycle()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}
