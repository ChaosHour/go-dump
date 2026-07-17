package dump

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestFileBuffer_Plain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plain.sql")

	b := NewFileBuffer(path, false, "", 0)
	if _, err := b.Write([]byte("INSERT INTO t VALUES (1);\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(data) != "INSERT INTO t VALUES (1);\n" {
		t.Errorf("unexpected content: %q", data)
	}
}

func TestFileBuffer_GzipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.sql")
	content := strings.Repeat("INSERT INTO t VALUES (1);\n", 1000)

	b := NewFileBuffer(path, true, CompressFormatGzip, 1)
	if _, err := b.Write([]byte(content)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path + ".gz")
	if err != nil {
		t.Fatalf("expected %s.gz to exist: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()
	got, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(got) != content {
		t.Errorf("gzip round-trip mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

func TestFileBuffer_ZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.sql")
	content := strings.Repeat("INSERT INTO t VALUES (1);\n", 1000)

	b := NewFileBuffer(path, true, CompressFormatZstd, 3)
	if _, err := b.Write([]byte(content)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path + ".zst")
	if err != nil {
		t.Fatalf("expected %s.zst to exist: %v", path, err)
	}
	defer f.Close()
	zr, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer zr.Close()
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(got) != content {
		t.Errorf("zstd round-trip mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

// Flush must push buffered data through the compressor far enough that a
// reader can decode everything written so far — this is what --resume and
// crash inspection rely on for partially-written files.
func TestFileBuffer_ZstdFlushMakesDataReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.sql")

	b := NewFileBuffer(path, true, CompressFormatZstd, 3)
	if _, err := b.Write([]byte("SELECT 1;\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	f, err := os.Open(path + ".zst")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	zr, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer zr.Close()
	got, _ := io.ReadAll(zr) // stream is unterminated; data must still be present
	if string(got) != "SELECT 1;\n" {
		t.Errorf("flushed data not readable: got %q", got)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCompressExtension(t *testing.T) {
	if got := CompressExtension(CompressFormatGzip); got != ".gz" {
		t.Errorf("gzip extension: got %q", got)
	}
	if got := CompressExtension(CompressFormatZstd); got != ".zst" {
		t.Errorf("zstd extension: got %q", got)
	}
	if got := CompressExtension(""); got != ".gz" {
		t.Errorf("default extension: got %q", got)
	}
}
