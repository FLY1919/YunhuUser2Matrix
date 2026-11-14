package webdist

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAssetURL(t *testing.T) {
	const full = "https://github.com/element-hq/element-web/releases/download/v1.2.3/element-v1.2.3.tar.gz"

	cases := []struct {
		name   string
		mirror string
		want   string
	}{
		{"no mirror", "", full},
		{"ghproxy prefix", "https://ghproxy.net", "https://ghproxy.net/" + full},
		{"prefix trailing slash", "https://ghproxy.net/", "https://ghproxy.net/" + full},
		{"host replacement", "https://mirror.example.com/github.com",
			"https://mirror.example.com/github.com/element-hq/element-web/releases/download/v1.2.3/element-v1.2.3.tar.gz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := assetURL(tc.mirror, "v1.2.3"); got != tc.want {
				t.Fatalf("assetURL(%q) = %q, want %q", tc.mirror, got, tc.want)
			}
		})
	}
}

func TestWithin(t *testing.T) {
	root := filepath.Clean("/srv/web")
	cases := []struct {
		target string
		want   bool
	}{
		{filepath.Join(root, "index.html"), true},
		{filepath.Join(root, "static", "js", "app.js"), true},
		{root, true},
		{filepath.Join(root, "..", "etc", "passwd"), false},
		{filepath.Join(root, ".."), false},
	}
	for _, tc := range cases {
		if got := within(root, tc.target); got != tc.want {
			t.Errorf("within(%q) = %v, want %v", tc.target, got, tc.want)
		}
	}
}

// buildTarGz writes an archive whose entries are named by paths, with a single
// "element-v1.0.0/" prefix, mirroring the real release layout.
func buildTarGz(t *testing.T, paths map[string]string) *bytes.Reader {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range paths {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(body)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(buf.Bytes())
}

func TestExtractTarStripsTopLevel(t *testing.T) {
	dest := t.TempDir()
	r := buildTarGz(t, map[string]string{
		"element-v1.0.0/index.html":         "<html>hi</html>",
		"element-v1.0.0/config.sample.json": "{}",
		"element-v1.0.0/bundles/app.js":     "console.log(1)",
	})

	gz, err := gzip.NewReader(r)
	if err != nil {
		t.Fatal(err)
	}
	files, _, err := extractTar(tar.NewReader(gz), dest)
	if err != nil {
		t.Fatalf("extractTar: %v", err)
	}
	if files != 3 {
		t.Fatalf("extracted %d files, want 3", files)
	}

	got, err := os.ReadFile(filepath.Join(dest, "index.html"))
	if err != nil {
		t.Fatalf("index.html not at destination root: %v", err)
	}
	if string(got) != "<html>hi</html>" {
		t.Fatalf("index.html = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dest, "bundles", "app.js")); err != nil {
		t.Fatalf("nested file missing: %v", err)
	}
}

func TestExtractTarRejectsTraversal(t *testing.T) {
	dest := t.TempDir()
	r := buildTarGz(t, map[string]string{
		"element-v1.0.0/../../evil.txt": "pwned",
	})

	gz, err := gzip.NewReader(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractTar(tar.NewReader(gz), dest); err == nil {
		t.Fatal("expected an error for a traversing archive entry")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dest), "evil.txt")); err == nil {
		t.Fatal("traversing entry escaped the destination directory")
	}
}

func TestFetchRejectsExistingBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Must fail before touching the network.
	if _, err := Fetch(context.Background(), Options{Dir: dir}); err == nil {
		t.Fatal("expected Fetch to refuse an existing build without Force")
	}
}
