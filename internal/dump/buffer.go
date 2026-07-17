package dump

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/ChaosHour/go-dump/internal/log"
)

const BufferTypeFile = "file"

// Compression formats accepted by --compress-format.
const (
	CompressFormatGzip = "gzip"
	CompressFormatZstd = "zstd"
)

// CompressExtension returns the file extension for a compression format.
func CompressExtension(format string) string {
	if format == CompressFormatZstd {
		return ".zst"
	}
	return ".gz"
}

type BufferOptions struct {
	Compress       bool
	CompressFormat string
	CompressLevel  int
	Type           string
	Path           string
}

// compressor is the writer interface shared by gzip.Writer and zstd.Encoder.
type compressor interface {
	Write(p []byte) (int, error)
	Flush() error
	Close() error
}

// Buffer is the default struct to write the data.
type Buffer struct {
	Type           string
	Buffer         *bufio.Writer
	Compressor     compressor
	FileDescriptor *os.File
}

func (b *Buffer) Write(p []byte) (int, error) {
	return b.Buffer.Write(p)
}

func (b *Buffer) Flush() error {
	if err := b.Buffer.Flush(); err != nil {
		return err
	}
	// For compressed buffers, also flush the compressor so the data reaches the
	// file descriptor rather than sitting in the compressor's internal state.
	if b.Compressor != nil {
		return b.Compressor.Flush()
	}
	return nil
}

func (b *Buffer) Close() error {
	if err := b.Flush(); err != nil {
		return err
	}
	if b.Compressor != nil {
		if err := b.Compressor.Close(); err != nil {
			return err
		}
	}
	return b.FileDescriptor.Close()
}

func NewBuffer(options *BufferOptions) (*Buffer, error) {
	if options.Type == BufferTypeFile {
		return NewFileBuffer(options.Path, options.Compress, options.CompressFormat, options.CompressLevel), nil
	}
	return nil, errors.New("Buffer type " + options.Type + " not supported.")
}

func NewFileBuffer(fileName string, compress bool, compressFormat string, compressLevel int) *Buffer {
	ext := CompressExtension(compressFormat)
	if compress && !strings.HasSuffix(fileName, ext) {
		fileName = fileName + ext
	}
	fileDescriptor, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating the file %s: %s", fileName, err.Error())
	}
	if compress {
		var cw compressor
		switch compressFormat {
		case CompressFormatZstd:
			// EncoderLevelFromZstd maps the zstd CLI's 1-19 scale onto the
			// library's speed presets.
			zw, err := zstd.NewWriter(fileDescriptor,
				zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressLevel)))
			if err != nil {
				log.Fatalf("Error getting zstd writer: %s", err.Error())
			}
			cw = zw
		default:
			gw, err := gzip.NewWriterLevel(fileDescriptor, compressLevel)
			if err != nil {
				log.Fatalf("Error getting gzip writer: %s", err.Error())
			}
			cw = gw
		}
		buffer := bufio.NewWriter(cw)
		return &Buffer{Type: BufferTypeFile, Buffer: buffer, Compressor: cw, FileDescriptor: fileDescriptor}
	}
	buffer := bufio.NewWriter(fileDescriptor)
	return &Buffer{Type: BufferTypeFile, Buffer: buffer, FileDescriptor: fileDescriptor}
}

func NewChunkBuffer(c *DataChunk, workerId int) (*Buffer, error) {
	var filename string
	if c.IsSingleChunk {
		filename = fmt.Sprintf("%s.sql", c.Task.Table.GetUnescapedFullName())
	} else {
		filename = fmt.Sprintf("%s-thread%d.sql", c.Task.Table.GetUnescapedFullName(), workerId)
	}
	fullpath := filepath.Join(c.Task.TaskManager.DestinationDir, filename)

	bufferOptions := c.Task.TaskManager.GetBufferOptions()
	bufferOptions.Path = fullpath

	buffer, err := NewBuffer(bufferOptions)
	if err != nil {
		return nil, err
	}

	// utf8mb4 supports the full Unicode range including 4-byte characters.
	fmt.Fprintf(buffer, "SET NAMES utf8mb4;\n")
	// max_allowed_packet is global-only in MySQL 8.0+ and cannot be set at session scope.
	// Configure it on the server (my.cnf) before restoring large tables.
	fmt.Fprintf(buffer, "SET TIME_ZONE='+00:00';\n")
	fmt.Fprintf(buffer, "SET UNIQUE_CHECKS=0;\n")
	fmt.Fprintf(buffer, "SET FOREIGN_KEY_CHECKS=0;\n")
	fmt.Fprintf(buffer, "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO';\n")

	return buffer, nil
}

func NewTableDefinitionBuffer(t *Task) (*Buffer, error) {
	bufferOptions := t.TaskManager.GetBufferOptions()
	bufferOptions.Path = fmt.Sprintf("%s/%s-definition.sql", t.TaskManager.DestinationDir, t.Table.GetUnescapedFullName())
	return NewBuffer(bufferOptions)
}

func NewMasterDataBuffer(t *TaskManager) (*Buffer, error) {
	bufferOptions := t.GetBufferOptions()
	bufferOptions.Path = fmt.Sprintf("%s/master-data.sql", t.DestinationDir)
	return NewBuffer(bufferOptions)
}

func NewSlaveDataBuffer(t *TaskManager) (*Buffer, error) {
	bufferOptions := t.GetBufferOptions()
	bufferOptions.Path = fmt.Sprintf("%s/slave-data.sql", t.DestinationDir)
	return NewBuffer(bufferOptions)
}
