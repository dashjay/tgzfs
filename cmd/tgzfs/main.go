package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"tgzfs/pkg/fusefs"

	"github.com/jacobsa/fuse"
)

var fTar = flag.String("tar-file", "", "tar file")
var fMountPoint = flag.String("mount_point", "", "Path to mount point.")
var fDebug = flag.Bool("debug", false, "Enable debug logging.")

func main() {
	flag.Parse()

	// Create an appropriate file system.
	server, err := fusefs.NewGzipFs(*fTar)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}

	// Mount the file system.
	if *fMountPoint == "" {
		log.Fatalf("You must set --mount_point.")
	}

	cfg := &fuse.MountConfig{
		ReadOnly: true,
	}

	if *fDebug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}

	mfs, err := fuse.Mount(*fMountPoint, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	c := make(chan os.Signal, 1)
	var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(c, shutdownSignals...)

	go func() {
		log.Println("Start Join")
		// Wait for it to be unmounted.
		if err = mfs.Join(context.Background()); err != nil {
			log.Fatalf("Join: %v", err)
		}
	}()
	<-c
	log.Println("Start Umount")
	err = fuse.Unmount(*fMountPoint)
	if err != nil {
		log.Fatalf("Umount: %v", err)
	}
}
