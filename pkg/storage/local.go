package storage

import (
	"os"
	"path/filepath"
)

func NewLocalStorage(rootDir string) Storage {
	return localStorage{rootDir}
}

type localStorage struct {
	rootDir string
}

type localFile struct {
	*os.File
}

func (f localFile) Name() string {
	return filepath.Base(f.File.Name())
}

func (s localStorage) Open(name string) (File, error) {
	file, err := os.Open(filepath.Join(s.rootDir, name))
	if err != nil {
		return nil, err
	}
	return localFile{file}, nil
}

func (s localStorage) Create(name string) (File, error) {
	file, err := os.Create(filepath.Join(s.rootDir, name))
	if err != nil {
		return nil, err
	}
	return localFile{file}, nil
}

func (s localStorage) Remove(name string) error {
	return os.Remove(filepath.Join(s.rootDir, name))
}
