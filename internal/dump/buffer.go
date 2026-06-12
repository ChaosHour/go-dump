package dump

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ChaosHour/go-dump/internal/log"
)

const BufferTypeGzipFile = "gzip"
const BufferTypeFile = "file"

type BufferOptions struct {
	Compress      bool
	CompressLevel int
	Type          string
	Path          string
}

// Buffer is the default struct to write the data.
type Buffer struct {
	Type           string
	Buffer         *bufio.Writer
	GzipWriter     *gzip.Writer
	FileDescriptor *os.File
}

func (b *Buffer) Write(p []byte) (int, error) {
	return b.Buffer.Write(p)
}

func (b *Buffer) Flush() error {
	if err := b.Buffer.Flush(); err != nil {
		return err
	}
	// For gzip buffers, also flush the compressor so the data reaches the file
	// descriptor rather than sitting in the gzip writer's internal state.
	if b.Type == BufferTypeGzipFile {
		return b.GzipWriter.Flush()
	}
	return nil
}

func (b *Buffer) Close() error {
	if err := b.Flush(); err != nil {
		return err
	}
	switch b.Type {
	case BufferTypeGzipFile:
		if err := b.GzipWriter.Close(); err != nil {
			return err
		}
		return b.FileDescriptor.Close()
	case BufferTypeFile:
		return b.FileDescriptor.Close()
	}
	return nil
}

func NewBuffer(options *BufferOptions) (*Buffer, error) {
	if options.Type == BufferTypeFile {
		return NewFileBuffer(options.Path, options.Compress, options.CompressLevel), nil
	}
	return nil, errors.New("Buffer type " + options.Type + " not supported.")
}

func NewFileBuffer(fileName string, compress bool, compressLevel int) *Buffer {
	if compress && !strings.HasSuffix(fileName, ".gz") {
		fileName = fileName + ".gz"
	}
	fileDescriptor, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating the file %s: %s", fileName, err.Error())
	}
	if compress {
		gzipWriter, err := gzip.NewWriterLevel(fileDescriptor, compressLevel)
		if err != nil {
			log.Fatalf("Error getting gzip writer: %s", err.Error())
		}
		buffer := bufio.NewWriter(gzipWriter)
		return &Buffer{Type: BufferTypeGzipFile, Buffer: buffer, GzipWriter: gzipWriter, FileDescriptor: fileDescriptor}
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
