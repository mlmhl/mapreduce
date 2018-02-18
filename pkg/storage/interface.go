package storage

type File interface {
	Name() string
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
}

type Storage interface {
	Open(name string) (File, error)
	Create(name string) (File, error)
	Remove(name string) error
}
