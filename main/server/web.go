package main

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// embeddedWeb contains the static export produced by the frontend project.
// Keeping it in the server binary makes the Web entry independent of its
// process working directory.
//
//go:embed web
var embeddedWeb embed.FS

func newWebHandler() http.Handler {
	root, err := fs.Sub(embeddedWeb, "web")
	if err != nil {
		panic("create embedded Web filesystem: " + err.Error())
	}
	files := http.FileServer(http.FS(root))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			writeError(w, http.StatusNotFound, "route_not_found", "route was not found")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("X-Content-Type-Options", "nosniff")
		switch {
		case r.URL.Path == "/" || r.URL.Path == "/index.html":
			w.Header().Set("Cache-Control", "no-cache")
		case strings.HasPrefix(r.URL.Path, "/assets/"):
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, r)
	})
}
