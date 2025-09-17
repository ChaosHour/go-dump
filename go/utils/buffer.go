package utils

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/outbrain/golib/log"

	"strings"
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

// Write a slice of bytes into the buffer.
func (b *Buffer) Write(p []byte) (int, error) {
	return b.Buffer.Write(p)
}

// Flush the buffer.
func (b *Buffer) Flush() error {
	return b.Buffer.Flush()
}

// Close execute the close statements for each buffer type.
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
	return nil, errors.New("Buffer type " + options.Type + " not susported.")
}

func NewFileBuffer(fileName string, compress bool, compressLevel int) *Buffer {
	var fileDescriptor *os.File
	var err error
	if compress && !strings.HasSuffix(fileName, ".gz") {
		fileName = fileName + ".gz"
	}

	fileDescriptor, err = os.Create(fileName)
	if err != nil {
		log.Fatalf("Error crating the file %s: %s", fileName, err.Error())
	}

	if compress {
		gzipWriter, err := gzip.NewWriterLevel(fileDescriptor, compressLevel)
		if err != nil {
			log.Fatalf("Error getting gzip writer: %s", err.Error())
		}
		buffer := bufio.NewWriter(gzipWriter)
		return &Buffer{Type: BufferTypeGzipFile, Buffer: buffer, GzipWriter: gzipWriter}
	}
	buffer := bufio.NewWriter(fileDescriptor)
	return &Buffer{Type: BufferTypeFile, Buffer: buffer}

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

	fmt.Fprintf(buffer, "SET NAMES utf8;\n")
	fmt.Fprintf(buffer, "SET GLOBAL MAX_ALLOWED_PACKET=1073741824;\n")
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
	filename := fmt.Sprintf("%s/master-data.sql", t.DestinationDir)

	bufferOptions := t.GetBufferOptions()
	bufferOptions.Path = filename

	return NewBuffer(bufferOptions)

}

func NewSlaveDataBuffer(t *TaskManager) (*Buffer, error) {
	filename := fmt.Sprintf("%s/slave-data.sql", t.DestinationDir)

	bufferOptions := t.GetBufferOptions()
	bufferOptions.Path = filename

	return NewBuffer(bufferOptions)

}
