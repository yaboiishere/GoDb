package internal

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

func createFileSync(file string) (int, error) {
	//get fg
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirFd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer func(fd int) {
		err := syscall.Close(fd)
		if err != nil {
			return

		}
	}(dirFd)

	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirFd, path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}

	if err = syscall.Fsync(fd); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}

	return fd, nil
}

func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}

	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}

	if err := updateRoot(db); err != nil {
		return err
	}

	return syscall.Fsync(db.fd)
}
